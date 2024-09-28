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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerRequestProto;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;

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
    ContainerProtos.ListBlockRequestProto.Builder listBlockBuilder = ContainerProtos.ListBlockRequestProto.newBuilder()
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

  ContainerCommandRequestProto createGetBlockRequest(DatanodeBlockID datanodeBlockId) {
    ContainerProtos.GetBlockRequestProto.Builder readBlockRequest = ContainerProtos.GetBlockRequestProto
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
