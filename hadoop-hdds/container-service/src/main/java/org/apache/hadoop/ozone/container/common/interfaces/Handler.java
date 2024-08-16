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

package org.apache.hadoop.ozone.container.common.interfaces;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.ratis.statemachine.StateMachine;

import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;

/**
 * Dispatcher sends ContainerCommandRequests to Handler. Each Container Type
 * should have an implementation for Handler.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class Handler {

  protected final ConfigurationSource conf;
  protected final ContainerSet containerSet;
  protected final VolumeSet volumeSet;
  protected String clusterId;
  protected final ContainerMetrics metrics;
  protected String datanodeId;
  private final IncrementalReportSender<Container> icrSender;

  protected Handler(ConfigurationSource config, String datanodeId, ContainerSet contSet, VolumeSet volumeSet,
      ContainerMetrics containerMetrics, IncrementalReportSender<Container> icrSender) {
    this.conf = config;
    this.containerSet = contSet;
    this.volumeSet = volumeSet;
    this.metrics = containerMetrics;
    this.datanodeId = datanodeId;
    this.icrSender = icrSender;
  }

  /**
   * Returns a handler for the specified container type.
   *
   * @param containerType the type of container for which the handler is required
   * @param config the configuration source
   * @param datanodeId the ID of the data node
   * @param contSet the set of containers
   * @param volumeSet the set of volumes
   * @param metrics metrics for the container
   * @param icrSender the incremental report sender
   * @return a Handler for the specified container type
   * @throws IllegalArgumentException if the container type does not exist
   */
  public static Handler getHandlerForContainerType(ContainerType containerType, ConfigurationSource config,
      String datanodeId, ContainerSet contSet, VolumeSet volumeSet, ContainerMetrics metrics,
      IncrementalReportSender<Container> icrSender) {
    if (Objects.requireNonNull(containerType) == ContainerType.KeyValueContainer) {
      return new KeyValueHandler(config, datanodeId, contSet, volumeSet, metrics, icrSender);
    }
    throw new IllegalArgumentException("Handler for ContainerType: " + containerType + "doesn't exist.");
  }

  /**
   * Retrieves the data channel stream for a given container based on the specified request message.
   *
   * @param container the container for which the data channel will be retrieved
   * @param msg the command request message associated with the data channel retrieval
   * @return the data channel stream corresponding to the given container and request message
   * @throws StorageContainerException if an error occurs while retrieving the data channel
   */
  public abstract StateMachine.DataChannel getStreamDataChannel(Container container, ContainerCommandRequestProto msg)
      throws StorageContainerException;

  /**
   * Returns the id of this datanode.
   *
   * @return datanode Id
   */
  protected String getDatanodeId() {
    return datanodeId;
  }

  /**
   * This should be called whenever there is state change. It will trigger an ICR to SCM.
   *
   * @param container Container for which ICR has to be sent
   */
  protected void sendICR(final Container container) throws StorageContainerException {
    if (container.getContainerState() == State.RECOVERING) {
      // Ignoring the recovering containers reports for now.
      return;
    }
    icrSender.send(container);
  }

  /**
   * Handles the given container command request.
   *
   * @param msg the container command request protocol message
   * @param container the container to be handled
   * @param dispatcherContext the context of the dispatcher handling the command
   * @return the response protocol for the executed command
   */
  public abstract ContainerCommandResponseProto handle(ContainerCommandRequestProto msg, Container container,
      DispatcherContext dispatcherContext);

  /**
   * Imports container from a raw input stream.
   */
  public abstract Container importContainer(ContainerData containerData, InputStream rawContainerStream,
      TarContainerPacker packer) throws IOException;

  /**
   * Exports container to the output stream.
   */
  public abstract void exportContainer(Container container, OutputStream outputStream, TarContainerPacker packer)
      throws IOException;

  /**
   * Stop the Handler.
   */
  public abstract void stop();

  /**
   * Marks the container for closing. Moves the container to {@link State#CLOSING} state.
   *
   * @param container container to update
   * @throws IOException in case of exception
   */
  public abstract void markContainerForClose(Container container) throws IOException;

  /**
   * Marks the container Unhealthy. Moves the container to {@link State#UNHEALTHY} state.
   *
   * @param container container to update
   * @param reason The reason the container was marked unhealthy
   * @throws IOException in case of exception
   */
  public abstract void markContainerUnhealthy(Container container, ScanResult reason) throws IOException;

  /**
   * Moves the Container to {@link State#QUASI_CLOSED} state.
   *
   * @param container container to be quasi closed
   * @param reason The reason the container was quasi closed, for logging purposes.
   */
  public abstract void quasiCloseContainer(Container container, String reason) throws IOException;

  /**
   * Moves the Container to {@link State#CLOSED} state.
   *
   * @param container container to be closed
   */
  public abstract void closeContainer(Container container) throws IOException;

  /**
   * Deletes the given container.
   *
   * @param container container to be deleted
   * @param force     if this is set to true, we delete container without checking state of the container.
   */
  public abstract void deleteContainer(Container container, boolean force) throws IOException;

  /**
   * Deletes the given files associated with a block of the container.
   *
   * @param container container whose block is to be deleted
   * @param blockData block to be deleted
   */
  public abstract void deleteBlock(Container container, BlockData blockData) throws IOException;

  /**
   * Deletes the possible onDisk but unreferenced blocks/chunks with localID in the container.
   *
   * @param container container whose block/chunk is to be deleted
   * @param localID   localId of the block/chunk
   */
  public abstract void deleteUnreferenced(Container container, long localID) throws IOException;

  /**
   * Adds a finalized block to a container.
   *
   * @param container The container to which the finalized block will be added.
   * @param localID The local identifier for the block.
   */
  public abstract void addFinalizedBlock(Container container, long localID);

  /**
   * Checks if a finalized block exists in the specified container with the given local ID.
   *
   * @param container the container to be checked
   * @param localID the local ID of the block to be verified
   * @return true if the finalized block exists, false otherwise
   */
  public abstract boolean isFinalizedBlockExist(Container container, long localID);

  /**
   * Sets the cluster ID for this handler.
   *
   * @param clusterID the new cluster ID to be set
   */
  public void setClusterID(String clusterID) {
    this.clusterId = clusterID;
  }
}
