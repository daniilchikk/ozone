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

package org.apache.hadoop.hdds.scm.client;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.EchoResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.FinalizeBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetCommittedBlockLengthResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Interface for communication with a datanode.
 * Provides methods to perform any protocol calls by Container clients on a single datanode.
 */
public interface ContainerApi extends AutoCloseable {
  ListBlockResponseProto listBlock(long containerId, Long startLocalId, int count) throws IOException;

  GetBlockResponseProto getBlock(BlockID blockId, Map<DatanodeDetails, Integer> replicaIndexes) throws IOException;

  GetCommittedBlockLengthResponseProto getCommittedBlockLength(BlockID blockId) throws IOException;

  PutBlockResponseProto putBlock() throws IOException;

  XceiverClientReply putBlockAsync(BlockData containerBlockData, boolean eof) throws IOException, ExecutionException, InterruptedException;

  FinalizeBlockResponseProto finalizeBlock(DatanodeBlockID blockId) throws IOException;

  ReadChunkResponseProto readChunk(ChunkInfo chunk, DatanodeBlockID blockId) throws IOException;

  WriteChunkRequestProto writeChunk() throws IOException;

  XceiverClientReply writeChunkAsync(ChunkInfo chunk, BlockID blockId, ByteString data, int replicationIndex,
      BlockData blockData, boolean close) throws IOException, ExecutionException, InterruptedException;

  default void createRecoveringContainer(long containerId, int replicaIndex) throws IOException {
    createContainer(containerId, ContainerDataProto.State.RECOVERING, replicaIndex);
  }

  default void createContainer(long containerId) throws IOException {
    createContainer(containerId, null, 0);
  }

  void createContainer(long containerId, @Nullable ContainerDataProto.State state, int replicaIndex) throws IOException;

  XceiverClientReply createContainerAsync() throws IOException;

  void deleteContainer(long containerId, boolean force) throws IOException;

  void closeContainer(long containerId) throws IOException;

  XceiverClientReply closeContainerAsync() throws IOException;

  ReadContainerResponseProto readContainer(long containerId) throws IOException;

  EchoResponseProto echo(long containerId, ByteString payloadReqBytes, int payloadRespSizeKB, int sleepTimeMs,
      boolean readOnly) throws IOException;

  CompletableFuture<XceiverClientReply> watchForCommit(long commitIndex);

  long getReplicatedMinCommitIndex();

  void updateCommitInfosMap(Collection<RaftProtos.CommitInfoProto> commitInfos);

  DataStreamOutput stream(ByteBuffer readOnlyByteBuffer);

  DataStreamOutput stream(ByteBuffer readOnlyByteBuffer, RoutingTable routingTable);

  @Override
  void close();
}
