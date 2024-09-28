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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.BlockNotCommittedException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.*;

public class ContainerApiImpl implements ContainerApi {

  private static final List<XceiverClientSpi.Validator> VALIDATORS = createValidators();

  private final XceiverClientSpi client;

  private final ContainerApiHelper requestHelper;

  public ContainerApiImpl(XceiverClientSpi client, @Nullable Token<? extends TokenIdentifier> token)
      throws IOException {
    this.client = client;

    String firstDatanodeUuid = client.getPipeline().getFirstNode().getUuidString();
    this.requestHelper = new ContainerApiHelper(firstDatanodeUuid, token);
  }

  @Override
  public ListBlockResponseProto listBlock(long containerId, Long startLocalId, int count) throws IOException {
    ContainerCommandRequestProto request = requestHelper.createListBlockRequest(containerId, startLocalId, count);

    return client.sendCommand(request, getValidatorList()).getListBlock();
  }

  private static List<XceiverClientSpi.Validator> getValidatorList() {
    return VALIDATORS;
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

  @Override
  public void close() {

  }
}
