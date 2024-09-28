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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerRequestProto;
import org.apache.hadoop.hdds.tracing.TracingUtil;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.GetBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadContainer;


/**
 * Class designed working with Datanode Proto requests and responses.
 */
class ContainerApiHelper {
  private final String datanodeUuid;

  ContainerApiHelper(String datanodeUuid) {
    this.datanodeUuid = datanodeUuid;
  }

  ContainerCommandRequestProto createGetBlockRequest(DatanodeBlockID datanodeBlockId, String token) {
    ContainerProtos.GetBlockRequestProto.Builder readBlockRequest = ContainerProtos.GetBlockRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockId);

    long containerId = datanodeBlockId.getContainerID();

    ContainerCommandRequestProto.Builder containerCommandBuilder =
        createContainerCommandRequestBuilder(GetBlock, containerId, token);

    containerCommandBuilder.setGetBlock(readBlockRequest);

    return containerCommandBuilder.build();
  }

  ContainerCommandRequestProto createReadContainerRequest(long containerId, String token) {
    ContainerCommandRequestProto.Builder containerCommandBuilder =
        createContainerCommandRequestBuilder(ReadContainer, containerId, token);

    containerCommandBuilder.setReadContainer(ReadContainerRequestProto.getDefaultInstance());

    return containerCommandBuilder.build();
  }

  private ContainerCommandRequestProto.Builder createContainerCommandRequestBuilder(ContainerProtos.Type type,
      long containerId, String token) {

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
