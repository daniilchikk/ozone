/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.*;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientSpi.Validator;
import org.apache.hadoop.hdds.scm.container.common.helpers.BlockNotCommittedException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Implementation of all container protocol calls performed by Container
 * clients.
 */
public final class ContainerProtocolCalls  {
  /**
   * There is no need to instantiate this class.
   */
  private ContainerProtocolCalls() {
  }

  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param encodedToken - encodedToken if security is enabled
   */
  public static void createContainer(XceiverClientSpi client, long containerID,
      String encodedToken) throws IOException {
    createContainer(client, containerID, encodedToken, null, 0);
  }
  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param encodedToken - encodedToken if security is enabled
   * @param state - state of the container
   * @param replicaIndex - index position of the container replica
   */
  public static void createContainer(XceiverClientSpi client,
      long containerID, String encodedToken,
      ContainerProtos.ContainerDataProto.State state, int replicaIndex)
      throws IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto.newBuilder();
    createRequest
        .setContainerType(ContainerProtos.ContainerType.KeyValueContainer);
    if (state != null) {
      createRequest.setState(state);
    }
    if (replicaIndex > 0) {
      createRequest.setReplicaIndex(replicaIndex);
    }

    String id = client.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      request.setTraceID(traceId);
    }

    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(createRequest.build());
    request.setDatanodeUuid(id);
    client.sendCommand(request.build(), getValidatorList());
  }

  /**
   * Deletes a container from a pipeline.
   *
   * @param force whether or not to forcibly delete the container.
   * @param encodedToken - encodedToken if security is enabled
   */
  public static void deleteContainer(XceiverClientSpi client, long containerID,
      boolean force, String encodedToken) throws IOException {
    ContainerProtos.DeleteContainerRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder();
    deleteRequest.setForceDelete(force);
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteContainer);
    request.setContainerID(containerID);
    request.setDeleteContainer(deleteRequest);
    request.setDatanodeUuid(id);
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      request.setTraceID(traceId);
    }
    client.sendCommand(request.build(), getValidatorList());
  }

  /**
   * Close a container.
   *
   * @param encodedToken - encodedToken if security is enabled
   */
  public static void closeContainer(XceiverClientSpi client,
      long containerID, String encodedToken)
      throws IOException {
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(CloseContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(id);
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      request.setTraceID(traceId);
    }
    client.sendCommand(request.build(), getValidatorList());
  }

  /**
   * readContainer call that gets meta data from an existing container.
   *
   * @param client       - client
   * @param encodedToken - encodedToken if security is enabled
   */
  public static ReadContainerResponseProto readContainer(
      XceiverClientSpi client, long containerID, String encodedToken)
      throws IOException {
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.ReadContainer);
    request.setContainerID(containerID);
    request.setReadContainer(ReadContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(id);
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      request.setTraceID(traceId);
    }
    ContainerCommandResponseProto response =
        client.sendCommand(request.build(), getValidatorList());

    return response.getReadContainer();
  }

  /**
   * Reads the data given the blockID.
   *
   * @param blockID - ID of the block
   * @param token a token for this block (may be null)
   * @return GetSmallFileResponseProto
   */
  public static GetSmallFileResponseProto readSmallFile(XceiverClientSpi client,
      BlockID blockID,
      Token<OzoneBlockTokenIdentifier> token) throws IOException {
    GetBlockRequestProto.Builder getBlock = GetBlockRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    ContainerProtos.GetSmallFileRequestProto getSmallFileRequest =
        GetSmallFileRequestProto
            .newBuilder().setBlock(getBlock)
            .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1)
            .build();
    String id = client.getPipeline().getClosestNode().getUuidString();

    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetSmallFile)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(id)
        .setGetSmallFile(getSmallFileRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      builder.setTraceID(traceId);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        client.sendCommand(request, getValidatorList());
    return response.getGetSmallFile();
  }

  /**
   * Send an echo to DataNode.
   *
   * @return EchoResponseProto
   */
  public static EchoResponseProto echo(XceiverClientSpi client, String encodedContainerID,
      long containerID, ByteString payloadReqBytes, int payloadRespSizeKB, int sleepTimeMs, boolean readOnly)
      throws IOException {
    ContainerProtos.EchoRequestProto getEcho =
        EchoRequestProto
            .newBuilder()
            .setPayload(payloadReqBytes)
            .setPayloadSizeResp(payloadRespSizeKB)
            .setSleepTimeMs(sleepTimeMs)
            .setReadOnly(readOnly)
            .build();
    String id = client.getPipeline().getClosestNode().getUuidString();

    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.Echo)
        .setContainerID(containerID)
        .setDatanodeUuid(id)
        .setEcho(getEcho);
    if (!encodedContainerID.isEmpty()) {
      builder.setEncodedToken(encodedContainerID);
    }
    String traceId = TracingUtil.exportCurrentSpan();
    if (traceId != null) {
      builder.setTraceID(traceId);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        client.sendCommand(request, getValidatorList());
    return response.getEcho();
  }

  /**
   * Validates a response from a container protocol call.  Any non-successful
   * return code is mapped to a corresponding exception and thrown.
   *
   * @param response container protocol call response
   * @throws StorageContainerException if the container protocol call failed
   */
  public static void validateContainerResponse(
      ContainerCommandResponseProto response
  ) throws StorageContainerException {
    if (response.getResult() == ContainerProtos.Result.SUCCESS) {
      return;
    } else if (response.getResult()
        == ContainerProtos.Result.BLOCK_NOT_COMMITTED) {
      throw new BlockNotCommittedException(response.getMessage());
    } else if (response.getResult()
        == ContainerProtos.Result.CLOSED_CONTAINER_IO) {
      throw new ContainerNotOpenException(response.getMessage());
    }
    throw new StorageContainerException(
        response.getMessage(), response.getResult());
  }

  private static List<Validator> getValidatorList() {
    return VALIDATORS;
  }

  private static final List<Validator> VALIDATORS = createValidators();

  private static List<Validator> createValidators() {
    return singletonList((request, response) -> validateContainerResponse(response));
  }

  public static List<Validator> toValidatorList(Validator validator) {
    final List<Validator> defaults = getValidatorList();
    final List<Validator> validators
        = new ArrayList<>(defaults.size() + 1);
    validators.addAll(defaults);
    validators.add(validator);
    return Collections.unmodifiableList(validators);
  }
}
