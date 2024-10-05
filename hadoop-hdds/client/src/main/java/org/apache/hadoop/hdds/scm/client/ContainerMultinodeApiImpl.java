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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.*;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link ContainerMultinodeApi} interface.
 * This class provides methods to perform protocol calls on multiple datanodes.
 */
public class ContainerMultinodeApiImpl implements ContainerMultinodeApi {

  private final XceiverClientSpi client;

  private final ContainerApiHelper requestHelper;

  public ContainerMultinodeApiImpl(XceiverClientSpi client, @Nullable Token<OzoneBlockTokenIdentifier> token)
      throws IOException {
    this.client = client;

    String firstDatanodeUuid = client.getPipeline().getFirstNode().getUuidString();
    this.requestHelper = new ContainerApiHelper(firstDatanodeUuid, token);
  }

  @Override
  public Map<DatanodeDetails, GetBlockResponseProto> getBlock(DatanodeBlockID datanodeBlockId)
      throws IOException, InterruptedException {

    ContainerCommandRequestProto request =
        requestHelper.createGetBlockRequest(datanodeBlockId);

    return client.sendCommandOnAllNodes(request)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().getGetBlock()
        ));
  }

  @Override
  public Map<DatanodeDetails, ReadContainerResponseProto> readContainer(long containerId)
      throws IOException, InterruptedException {

    ContainerCommandRequestProto request = requestHelper.createReadContainerRequest(containerId);

    return client.sendCommandOnAllNodes(request)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().getReadContainer()
        ));
  }

  @Override
  public void close() {
  }
}
