/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.protocolPB;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DataBuffers;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.utils.BufferUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

import static org.apache.hadoop.hdds.scm.utils.ClientCommandsUtils.getReadChunkVersion;

/**
 * A set of helper functions to create responses to container commands.
 */
public final class ContainerCommandResponseBuilders {

  /**
   * Returns a Container Command Response Builder with the specified result and message.
   *
   * @param request request message.
   * @param result result of the command.
   * @param message response message.
   * @return ContainerCommand Response Builder.
   */
  public static Builder getContainerCommandResponse(ContainerCommandRequestProto request, Result result,
      String message) {

    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(request.getCmdType())
        .setTraceID(request.getTraceID())
        .setResult(result)
        .setMessage(message);
  }

  /**
   * Returns a Container Command Response Builder.
   * This call is used to build success responses.
   * Calling function can add other fields to the response as required.
   *
   * @param request request message.
   * @return ContainerCommand Response Builder with a result as {@link Result#SUCCESS}.
   */
  public static Builder getSuccessResponseBuilder(ContainerCommandRequestProto request) {
    return ContainerCommandResponseProto.newBuilder()
        .setCmdType(request.getCmdType())
        .setTraceID(request.getTraceID())
        .setResult(Result.SUCCESS);
  }

  /**
   * Returns a Container Command Response. This call is used for creating {@code null} success responses.
   *
   * @param request request message.
   * @return ContainerCommand Response with a result as {@link Result#SUCCESS}.
   */
  public static ContainerCommandResponseProto getSuccessResponse(ContainerCommandRequestProto request) {
    return getSuccessResponseBuilder(request)
        .setMessage("")
        .build();
  }

  /**
   * We found a command type but no associated payload for the command.
   * Hence, return malformed Command as response.
   *
   * @param request - Protobuf message.
   * @return ContainerCommand Response with a result as {@link Result#MALFORMED_REQUEST}.
   */
  public static ContainerCommandResponseProto malformedRequest(ContainerCommandRequestProto request) {
    return getContainerCommandResponse(request, Result.MALFORMED_REQUEST, "Cmd type does not match the payload.")
        .build();
  }

  /**
   * We found a command type not supported yet.
   *
   * @param request - Protobuf message.
   * @return ContainerCommand Response with a result as {@link Result#UNSUPPORTED_REQUEST}.
   */
  public static ContainerCommandResponseProto unsupportedRequest(ContainerCommandRequestProto request) {
    return getContainerCommandResponse(request, Result.UNSUPPORTED_REQUEST, "Server does not support this command yet.")
        .build();
  }

  /**
   * Returns successful putBlock response.
   *
   * @param msg - Request.
   * @return Successful PutBlock response.
   */
  public static ContainerCommandResponseProto putBlockResponseSuccess(ContainerCommandRequestProto msg,
      BlockData blockData) {

    PutBlockResponseProto.Builder putBlock = PutBlockResponseProto.newBuilder()
        .setCommittedBlockLength(getCommittedBlockLengthResponseBuilder(blockData.getSize(), blockData.getBlockID()));

    return getSuccessResponseBuilder(msg)
        .setPutBlock(putBlock)
        .build();
  }

  /**
   * Generates a successful response containing block data for the given request.
   *
   * @param msg The request message.
   * @param data The block data to be included in the response.
   * @return A ContainerCommandResponseProto object containing the block data.
   */
  public static ContainerCommandResponseProto getBlockDataResponse(ContainerCommandRequestProto msg, BlockData data) {
    GetBlockResponseProto.Builder getBlock = GetBlockResponseProto.newBuilder()
        .setBlockData(data);

    return getSuccessResponseBuilder(msg)
        .setGetBlock(getBlock)
        .build();
  }

  /**
   * Generates a response containing a list of block data for the given request.
   *
   * @param msg The request message.
   * @param data The list of block data to be included in the response.
   * @return A ContainerCommandResponseProto object containing the list of block data.
   */
  public static ContainerCommandResponseProto getListBlockResponse(ContainerCommandRequestProto msg,
      List<BlockData> data) {

    ListBlockResponseProto.Builder builder = ListBlockResponseProto.newBuilder();
    builder.addAllBlockData(data);
    return getSuccessResponseBuilder(msg)
        .setListBlock(builder)
        .build();
  }

  /**
   * Generates a response based on the provided request message and block length.
   *
   * @param msg The request message of type ContainerCommandRequestProto.
   * @param blockLength The length of the block to be included in the response.
   * @return A ContainerCommandResponseProto object containing the block length information.
   */
  public static ContainerCommandResponseProto getBlockLengthResponse(ContainerCommandRequestProto msg,
      long blockLength) {

    GetCommittedBlockLengthResponseProto.Builder committedBlockLength =
        getCommittedBlockLengthResponseBuilder(blockLength, msg.getGetCommittedBlockLength().getBlockID());

    return getSuccessResponseBuilder(msg)
        .setGetCommittedBlockLength(committedBlockLength)
        .build();
  }

  /**
   * Constructs a GetCommittedBlockLengthResponseProto.Builder with the specified block length and block ID.
   *
   * @param blockLength The length of the block to be included in the response.
   * @param blockID The ID of the block to be included in the response.
   * @return The GetCommittedBlockLengthResponseProto.Builder object containing the block length and block ID.
   */
  public static GetCommittedBlockLengthResponseProto.Builder getCommittedBlockLengthResponseBuilder(long blockLength,
      DatanodeBlockID blockID) {

    return GetCommittedBlockLengthResponseProto.newBuilder()
        .setBlockLength(blockLength)
        .setBlockID(blockID);
  }

  /**
   * Generates a successful response for the PutSmallFile operation.
   *
   * @param msg The request message containing the PutSmallFile command.
   * @param blockData The block data associated with the PutSmallFile operation.
   * @return A ContainerCommandResponseProto object indicating the success of the PutSmallFile operation
   *    and containing relevant response data.
   */
  public static ContainerCommandResponseProto getPutFileResponseSuccess(ContainerCommandRequestProto msg,
      BlockData blockData) {

    PutSmallFileResponseProto.Builder putSmallFile = PutSmallFileResponseProto.newBuilder()
        .setCommittedBlockLength(getCommittedBlockLengthResponseBuilder(blockData.getSize(), blockData.getBlockID()));

    return getSuccessResponseBuilder(msg)
        .setCmdType(Type.PutSmallFile)
        .setPutSmallFile(putSmallFile)
        .build();
  }

  /**
   * Generates a successful response for a WriteChunk operation.
   *
   * @param msg The request message containing the WriteChunk command.
   * @param blockData The block data associated with the WriteChunk operation.
   * @return A ContainerCommandResponseProto object indicating the success of the WriteChunk operation
   *     and containing relevant response data.
   */
  public static ContainerCommandResponseProto getWriteChunkResponseSuccess(ContainerCommandRequestProto msg,
      BlockData blockData) {

    WriteChunkResponseProto.Builder writeChunk = WriteChunkResponseProto.newBuilder();
    if (blockData != null) {
      writeChunk
          .setCommittedBlockLength(getCommittedBlockLengthResponseBuilder(blockData.getSize(), blockData.getBlockID()));
    }

    return getSuccessResponseBuilder(msg)
        .setCmdType(Type.WriteChunk)
        .setWriteChunk(writeChunk)
        .build();
  }

  /**
   * Generates a successful response for the GetSmallFile operation.
   *
   * @param request The request message containing the GetSmallFile command.
   * @param dataBuffers A list of ByteString objects containing the data buffers for the small file.
   * @param info The ChunkInfo object containing metadata about the chunk.
   * @return A ContainerCommandResponseProto object indicating the success of the GetSmallFile operation
   *     and containing relevant response data.
   */
  public static ContainerCommandResponseProto getGetSmallFileResponseSuccess(ContainerCommandRequestProto request,
      List<ByteString> dataBuffers, ChunkInfo info) {

    Preconditions.checkNotNull(request);

    boolean isReadChunkV0 = getReadChunkVersion(request.getGetSmallFile()).equals(ContainerProtos.ReadChunkVersion.V0);

    ReadChunkResponseProto.Builder readChunk;

    if (isReadChunkV0) {
      // V0 has all response data in a single ByteBuffer
      ByteString combinedData = BufferUtils.concatByteStrings(dataBuffers);

      readChunk = ReadChunkResponseProto.newBuilder()
          .setChunkData(info)
          .setData(combinedData)
          .setBlockID(request.getGetSmallFile().getBlock().getBlockID());
    } else {
      // V1 splits response data into a list of ByteBuffers
      readChunk = ReadChunkResponseProto.newBuilder()
          .setChunkData(info)
          .setDataBuffers(DataBuffers.newBuilder()
              .addAllBuffers(dataBuffers)
              .build())
          .setBlockID(request.getGetSmallFile().getBlock().getBlockID());
    }

    GetSmallFileResponseProto.Builder getSmallFile = GetSmallFileResponseProto.newBuilder().setData(readChunk);

    return getSuccessResponseBuilder(request)
        .setCmdType(Type.GetSmallFile)
        .setGetSmallFile(getSmallFile)
        .build();
  }

  /**
   * Generates a response containing the requested container data.
   *
   * @param request The request message of type ContainerCommandRequestProto.
   * @param containerData The container data to be included in the response.
   * @return A ContainerCommandResponseProto object with the container data.
   */
  public static ContainerCommandResponseProto getReadContainerResponse(ContainerCommandRequestProto request,
      ContainerDataProto containerData) {

    Preconditions.checkNotNull(containerData);

    ReadContainerResponseProto.Builder response = ReadContainerResponseProto.newBuilder()
        .setContainerData(containerData);

    return getSuccessResponseBuilder(request)
        .setReadContainer(response)
        .build();
  }

  /**
   * Generates a response for a ReadChunk request, containing the requested chunk data.
   *
   * @param request The ContainerCommandRequestProto object containing the ReadChunk request.
   * @param data The ChunkBuffer containing the data to be included in the response.
   * @param byteBufferToByteString Function to convert ByteBuffer objects to ByteString.
   * @return A ContainerCommandResponseProto object containing the response data.
   */
  public static ContainerCommandResponseProto getReadChunkResponse(ContainerCommandRequestProto request,
      ChunkBuffer data, Function<ByteBuffer, ByteString> byteBufferToByteString) {

    boolean isReadChunkV0 = getReadChunkVersion(request.getReadChunk()).equals(ContainerProtos.ReadChunkVersion.V0);

    ReadChunkResponseProto.Builder response;

    if (isReadChunkV0) {
      // V0 has all response data in a single ByteBuffer
      response = ReadChunkResponseProto.newBuilder()
          .setChunkData(request.getReadChunk().getChunkData())
          .setData(data.toByteString(byteBufferToByteString))
          .setBlockID(request.getReadChunk().getBlockID());
    } else {
      // V1 splits response data into a list of ByteBuffers
      response = ReadChunkResponseProto.newBuilder()
          .setChunkData(request.getReadChunk().getChunkData())
          .setDataBuffers(DataBuffers.newBuilder()
              .addAllBuffers(data.toByteStringList(byteBufferToByteString))
              .build())
          .setBlockID(request.getReadChunk().getBlockID());
    }

    return getSuccessResponseBuilder(request)
        .setReadChunk(response)
        .build();
  }

  /**
   * Generates a successful response for the FinalizeBlock operation.
   *
   * @param msg The request message containing the FinalizeBlock command.
   * @param data The block data associated with the FinalizeBlock operation.
   * @return A ContainerCommandResponseProto object indicating the success of the FinalizeBlock operation
   * and containing relevant response data.
   */
  public static ContainerCommandResponseProto getFinalizeBlockResponse(ContainerCommandRequestProto msg,
      BlockData data) {

    ContainerProtos.FinalizeBlockResponseProto.Builder blockData =
        ContainerProtos.FinalizeBlockResponseProto.newBuilder()
        .setBlockData(data);

    return getSuccessResponseBuilder(msg)
        .setFinalizeBlock(blockData)
        .build();
  }

  /**
   * Generates an echo response based on the provided request message.
   * The response contains a random payload of the specified size and optionally simulates a delay before responding.
   *
   * @param msg The request message of type ContainerCommandRequestProto,
   *     containing the EchoRequest with payload size and optional sleep time.
   * @return A ContainerCommandResponseProto object containing the echo response with a random payload.
   */
  public static ContainerCommandResponseProto getEchoResponse(ContainerCommandRequestProto msg) {
    ContainerProtos.EchoRequestProto echoRequest = msg.getEcho();
    int responsePayload = echoRequest.getPayloadSizeResp();

    int sleepTimeMs = echoRequest.getSleepTimeMs();
    try {
      if (sleepTimeMs > 0) {
        Thread.sleep(sleepTimeMs);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    ContainerProtos.EchoResponseProto.Builder echo = ContainerProtos.EchoResponseProto
        .newBuilder()
        .setPayload(UnsafeByteOperations.unsafeWrap(RandomUtils.nextBytes(responsePayload)));

    return getSuccessResponseBuilder(msg)
        .setEcho(echo)
        .build();
  }

  private ContainerCommandResponseBuilders() {
    throw new UnsupportedOperationException("no instances");
  }
}
