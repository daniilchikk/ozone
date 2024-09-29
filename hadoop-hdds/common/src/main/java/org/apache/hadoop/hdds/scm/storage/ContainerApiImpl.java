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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.*;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.BlockNotCommittedException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;

public class ContainerApiImpl implements ContainerApi {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerApiImpl.class);

  private static final List<XceiverClientSpi.Validator> DEFAULT_VALIDATORS = createValidators();

  private final XceiverClientSpi client;

  private final ContainerApiHelper requestHelper;

  private final List<XceiverClientSpi.Validator> validators;

  public ContainerApiImpl(XceiverClientSpi client, @Nullable Token<? extends TokenIdentifier> token)
      throws IOException {
    this(client, token, DEFAULT_VALIDATORS);
  }

  public ContainerApiImpl(XceiverClientSpi client, @Nullable Token<? extends TokenIdentifier> token,
      List<XceiverClientSpi.Validator> validators) throws IOException {
    this.client = client;

    String firstDatanodeUuid = client.getPipeline().getFirstNode().getUuidString();
    this.requestHelper = new ContainerApiHelper(firstDatanodeUuid, token);
    this.validators = validators;
  }

  @Override
  public ListBlockResponseProto listBlock(long containerId, Long startLocalId, int count) throws IOException {
    ContainerCommandRequestProto request = requestHelper.createListBlockRequest(containerId, startLocalId, count);

    return client.sendCommand(request, validators).getListBlock();
  }

  @Override
  public GetBlockResponseProto getBlock(BlockID blockId, Map<DatanodeDetails, Integer> replicaIndexes)
      throws IOException {
    return tryEachDatanode(client.getPipeline(),
        d -> getBlock(blockId, d, replicaIndexes),
        d -> toErrorMessage(blockId, d));
  }

  @Override
  public ContainerProtos.GetCommittedBlockLengthResponseProto getCommittedBlockLength(BlockID blockId) throws IOException {
    ContainerCommandRequestProto request = requestHelper.createGetCommittedBlockLengthRequest(blockId);

    return client.sendCommand(request, validators).getGetCommittedBlockLength();
  }

  @Override
  public XceiverClientReply putBlockAsync(BlockData containerBlockData, boolean eof)
      throws IOException, ExecutionException, InterruptedException {
    ContainerCommandRequestProto request = requestHelper.createPutBlockRequest(containerBlockData, eof);

    return client.sendCommandAsync(request);
  }

  @Override
  public FinalizeBlockResponseProto finalizeBlock(DatanodeBlockID blockId) throws IOException {
    ContainerCommandRequestProto request = requestHelper.createFinalizeBlockRequest(blockId);

    return client.sendCommand(request, validators).getFinalizeBlock();
  }

  @Override
  public ReadChunkResponseProto readChunk(ChunkInfo chunk, DatanodeBlockID blockId) throws IOException {
    Span span = GlobalTracer.get()
        .buildSpan("readChunk")
        .start();
    try (Scope ignored = GlobalTracer.get().activateSpan(span)) {
      span.setTag("offset", chunk.getOffset())
          .setTag("length", chunk.getLen())
          .setTag("block", blockId.toString());
      return tryEachDatanode(client.getPipeline(),
          d -> readChunk(chunk, blockId, d),
          d -> toErrorMessage(chunk, blockId, d));
    } finally {
      span.finish();
    }
  }

  private GetBlockResponseProto getBlock(BlockID blockId, DatanodeDetails datanode,
      Map<DatanodeDetails, Integer> replicaIndexes) throws IOException {

    ContainerCommandRequestProto request =
        requestHelper.createGetBlockRequest(blockId.getContainerID(), blockId, replicaIndexes, datanode);

    return client.sendCommand(request, validators).getGetBlock();
  }

  private ReadChunkResponseProto readChunk(ChunkInfo chunk, DatanodeBlockID blockId, DatanodeDetails datanode)
      throws IOException {
    ContainerCommandRequestProto request = requestHelper.createReadChunkRequest(chunk, blockId);

    ReadChunkResponseProto response = client.sendCommand(request, validators).getReadChunk();

    final long readLen = getLen(response);
    if (readLen != chunk.getLen()) {
      throw new IOException(toErrorMessage(chunk, blockId, datanode) + ": readLen=" + readLen);
    }

    return response;
  }

  private static List<XceiverClientSpi.Validator> createValidators() {
    return singletonList((request, response) -> validateContainerResponse(response));
  }

  private static void validateContainerResponse(ContainerCommandResponseProto response) throws StorageContainerException {
    if (response.getResult() == BLOCK_NOT_COMMITTED) {
      throw new BlockNotCommittedException(response.getMessage());
    } else if (response.getResult() == CLOSED_CONTAINER_IO) {
      throw new ContainerNotOpenException(response.getMessage());
    } else if (response.getResult() != SUCCESS) {
      throw new StorageContainerException(response.getMessage(), response.getResult());
    }
  }
  static <T> T tryEachDatanode(Pipeline pipeline, CheckedFunction<DatanodeDetails, T, IOException> operation,
      Function<DatanodeDetails, String> toErrorMessage) throws IOException {

    IOException lastException = null;

    for (DatanodeDetails datanode : pipeline.getNodesInOrder()) {
      try {
        return operation.apply(datanode);
      } catch (IOException e) {
        Span span = GlobalTracer.get().activeSpan();
        if (e instanceof StorageContainerException) {
          StorageContainerException sce = (StorageContainerException) e;
          // Block token expired. There's no point retrying other DN.
          // Throw the exception to request a new block token right away.
          if (sce.getResult() == BLOCK_TOKEN_VERIFICATION_FAILED) {
            span.log("block token verification failed at DN " + datanode);
            throw e;
          }
        }
        span.log("failed to connect to DN " + datanode);
        LOG.warn("{}; will try another datanode.", toErrorMessage.apply(datanode), e);
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      throw new IOException("No datanode available");
    }
  }

  private static long getLen(ReadChunkResponseProto response) {
    if (response.hasData()) {
      return response.getData().size();
    } else if (response.hasDataBuffers()) {
      return response.getDataBuffers().getBuffersList().stream()
          .mapToLong(ByteString::size)
          .sum();
    } else {
      return -1;
    }
  }

  private static String toErrorMessage(BlockID blockId, DatanodeDetails datanode) {
    return String.format("Failed to get block #%s in container #%s from %s",
        blockId.getLocalID(),
        blockId.getContainerID(),
        datanode);
  }

  private static String toErrorMessage(ChunkInfo chunk, DatanodeBlockID blockId, DatanodeDetails datanode) {
    return String.format("Failed to read chunk %s (len=%s) %s from %s",
        chunk.getChunkName(),
        chunk.getLen(),
        blockId,
        datanode);
  }

  @Override
  public void close() {

  }
}
