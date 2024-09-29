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

package org.apache.hadoop.hdds.scm.storage;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.*;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.*;


/**
 * Class designed working with Datanode Proto requests and responses.
 */
class ContainerApiHelper {
  private final String datanodeUuid;

  private final String token;

  ContainerApiHelper(String datanodeUuid, @Nullable Token<? extends TokenIdentifier> token) throws IOException {
    this.datanodeUuid = datanodeUuid;

    if (token != null) {
      this.token = token.encodeToUrlString();
    } else {
      this.token = null;
    }
  }

  ContainerCommandRequestProto createListBlockRequest(long containerId, Long startLocalId, int count) {
    ListBlockRequestProto.Builder listBlockBuilder = ListBlockRequestProto.newBuilder()
        .setCount(count);

    if (startLocalId != null) {
      listBlockBuilder.setStartLocalID(startLocalId);
    }

    ContainerCommandRequestProto.Builder containerCommandBuilder =
        createContainerCommandRequestBuilder(ListBlock, containerId);

    containerCommandBuilder
            .setContainerID(containerId)
            .setListBlock(listBlockBuilder.build());

    return containerCommandBuilder.build();
  }

  ContainerCommandRequestProto createGetBlockRequest(long containerId, BlockID blockId,
      Map<DatanodeDetails, Integer> replicaIndexes, DatanodeDetails datanode) {

    DatanodeBlockID.Builder datanodeBlockID = blockId.getDatanodeBlockIDProtobufBuilder();
    int replicaIndex = replicaIndexes.getOrDefault(datanode, 0);
    if (replicaIndex > 0) {
      datanodeBlockID.setReplicaIndex(replicaIndex);
    }

    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto.newBuilder()
        .setBlockID(datanodeBlockID.build());

    return createContainerCommandRequestBuilder(GetBlock, containerId)
            .setGetBlock(readBlockRequest)
            .build();
  }

  ContainerCommandRequestProto createGetBlockRequest(DatanodeBlockID datanodeBlockId) {
    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockId);

    long containerId = datanodeBlockId.getContainerID();

    ContainerCommandRequestProto.Builder containerCommandBuilder =
        createContainerCommandRequestBuilder(GetBlock, containerId);

    containerCommandBuilder.setGetBlock(readBlockRequest);

    return containerCommandBuilder.build();
  }

  ContainerCommandRequestProto createReadContainerRequest(long containerId) {
    ContainerCommandRequestProto.Builder containerCommandBuilder =
        createContainerCommandRequestBuilder(ReadContainer, containerId);

    containerCommandBuilder.setReadContainer(ReadContainerRequestProto.getDefaultInstance());

    return containerCommandBuilder.build();
  }

  ContainerCommandRequestProto createGetCommittedBlockLengthRequest(BlockID blockId) {
    GetCommittedBlockLengthRequestProto.Builder getBlockLengthRequest =
        GetCommittedBlockLengthRequestProto.newBuilder().
            setBlockID(blockId.getDatanodeBlockIDProtobuf());

    return createContainerCommandRequestBuilder(GetCommittedBlockLength, blockId.getContainerID())
        .setGetCommittedBlockLength(getBlockLengthRequest)
        .build();
  }

  ContainerCommandRequestProto createPutBlockRequest(BlockData containerBlockData, boolean eof) {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData)
            .setEof(eof);

    return createContainerCommandRequestBuilder(PutBlock, containerBlockData.getBlockID().getContainerID())
        .setPutBlock(createBlockRequest)
        .build();
  }

  public ContainerCommandRequestProto createFinalizeBlockRequest(DatanodeBlockID blockId) {
    FinalizeBlockRequestProto.Builder finalizeBlockRequest = FinalizeBlockRequestProto.newBuilder().setBlockID(blockId);

    return createContainerCommandRequestBuilder(FinalizeBlock, blockId.getContainerID())
        .setFinalizeBlock(finalizeBlockRequest)
        .build();
  }

  public ContainerCommandRequestProto createReadChunkRequest(ChunkInfo chunk, DatanodeBlockID blockId) {
    ReadChunkRequestProto readChunkRequest =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(blockId)
            .setChunkData(chunk)
            .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1)
            .build();

    return createContainerCommandRequestBuilder(PutBlock, blockId.getContainerID())
        .setReadChunk(readChunkRequest)
        .build();
  }

  public ContainerCommandRequestProto createWriteChunkRequest(ChunkInfo chunk, BlockID blockId, ByteString data,
      int replicationIndex, BlockData blockData, boolean close) {
    long containerId = blockId.getContainerID();

    WriteChunkRequestProto.Builder writeChunkRequest =
        WriteChunkRequestProto.newBuilder()
            .setBlockID(DatanodeBlockID.newBuilder()
                .setContainerID(containerId)
                .setLocalID(blockId.getLocalID())
                .setBlockCommitSequenceId(blockId.getBlockCommitSequenceId())
                .setReplicaIndex(replicationIndex)
                .build())
            .setChunkData(chunk)
            .setData(data);

    if (blockData != null) {
      PutBlockRequestProto.Builder createBlockRequest =
          PutBlockRequestProto.newBuilder()
              .setBlockData(blockData)
              .setEof(close);
      writeChunkRequest.setBlock(createBlockRequest);
    }

    return createContainerCommandRequestBuilder(WriteChunk, containerId)
        .setWriteChunk(writeChunkRequest.build())
        .build();
  }

  private ContainerCommandRequestProto.Builder createContainerCommandRequestBuilder(ContainerProtos.Type type,
      long containerId) {

    ContainerCommandRequestProto.Builder containerCommandBuilder = ContainerCommandRequestProto.newBuilder()
        .setCmdType(type)
        .setContainerID(containerId)
        .setDatanodeUuid(datanodeUuid);

    if (token != null) {
      containerCommandBuilder.setEncodedToken(token);
    }

    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      containerCommandBuilder.setTraceID(traceId);
    }

    return containerCommandBuilder;
  }
}
