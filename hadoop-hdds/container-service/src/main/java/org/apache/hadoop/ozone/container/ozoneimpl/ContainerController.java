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
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.protocol.datanode.proto
    .ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.container.common.interfaces.Container.ScanResult;

/**
 * Control plane for container management in datanode.
 */
public class ContainerController {

  private final ContainerSet containerSet;
  private final Map<ContainerType, Handler> handlers;
  private static final Logger LOG = LoggerFactory.getLogger(ContainerController.class);

  public ContainerController(final ContainerSet containerSet, final Map<ContainerType, Handler> handlers) {
    this.containerSet = containerSet;
    this.handlers = handlers;
  }

  /**
   * Returns the Container given a container id.
   *
   * @param containerId ID of the container
   * @return Container
   */
  public Container getContainer(final long containerId) {
    return containerSet.getContainer(containerId);
  }

  /**
   * Retrieves the file path location of the specified container.
   *
   * @param containerId the ID of the container to find
   * @return the container file path if the container exists, otherwise "nonexistent"
   */
  public String getContainerLocation(final long containerId) {
    Container cont = containerSet.getContainer(containerId);
    if (cont != null) {
      return cont.getContainerData().getContainerPath();
    } else {
      return "nonexistent";
    }
  }

  /**
   * Marks the container for closing. Moves the container to CLOSING state.
   *
   * @param containerId Id of the container to update
   * @throws IOException in case of exception
   */
  public void markContainerForClose(final long containerId) throws IOException {
    Container container = containerSet.getContainer(containerId);
    if (container == null) {
      String warning;
      Set<Long> missingContainerSet = containerSet.getMissingContainerSet();
      if (missingContainerSet.contains(containerId)) {
        warning = "The Container is in the MissingContainerSet hence we can't close it. ContainerID: " + containerId;
      } else {
        warning = "The Container is not found. ContainerID: " + containerId;
      }
      LOG.warn(warning);
      throw new ContainerNotFoundException(warning);
    } else {
      if (container.getContainerState() == State.OPEN) {
        getHandler(container).markContainerForClose(container);
      }
    }
  }

  /**
   * Marks the container as UNHEALTHY.
   *
   * @param containerId Id of the container to update
   * @param reason The reason the container was marked unhealthy
   * @throws IOException in case of exception
   */
  public void markContainerUnhealthy(final long containerId, ScanResult reason) throws IOException {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      getHandler(container).markContainerUnhealthy(container, reason);
    } else {
      LOG.warn("Container {} not found, may be deleted, skip mark UNHEALTHY", containerId);
    }
  }

  /**
   * Returns the container report.
   *
   * @return ContainerReportsProto
   * @throws IOException in case of exception
   */
  public ContainerReportsProto getContainerReport() throws IOException {
    return containerSet.getContainerReport();
  }

  /**
   * Quasi closes a container given its id.
   *
   * @param containerId Id of the container to quasi close
   * @param reason The reason the container was quasi closed, for logging purposes.
   * @throws IOException in case of exception
   */
  public void quasiCloseContainer(final long containerId, String reason) throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).quasiCloseContainer(container, reason);
  }

  /**
   * Closes a container given its id.
   *
   * @param containerId Id of the container to close
   * @throws IOException in case of exception
   */
  public void closeContainer(final long containerId) throws IOException {
    final Container container = containerSet.getContainer(containerId);
    getHandler(container).closeContainer(container);
  }

  /**
   * Returns the Container given a container id.
   *
   * @param containerId ID of the container
   */
  public void addFinalizedBlock(final long containerId, final long localId) {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      getHandler(container).addFinalizedBlock(container, localId);
    }
  }

  /**
   * Checks if a finalized block exists within a specified container.
   *
   * @param containerId ID of the container to check
   * @param localId ID of the block to check within the container
   * @return {@code true} if the finalized block exists, {@code false} otherwise
   */
  public boolean isFinalizedBlockExist(final long containerId, final long localId) {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      return getHandler(container).isFinalizedBlockExist(container, localId);
    }
    return false;
  }

  /**
   * Imports a container based on the provided container data, raw container stream,
   * and tar container packer.
   *
   * @param containerData the data representing the container to be imported
   * @param rawContainerStream the InputStream containing the raw container data
   * @param packer the TarContainerPacker used to handle the container import
   * @return the imported Container
   * @throws IOException if an I/O error occurs during the import process
   */
  public Container importContainer(final ContainerData containerData, final InputStream rawContainerStream,
      final TarContainerPacker packer) throws IOException {
    return handlers.get(containerData.getContainerType()).importContainer(containerData, rawContainerStream, packer);
  }

  /**
   * Exports a container of the specified type and ID to the provided output stream,
   * using the given tar container packer.
   *
   * @param type the type of the container to be exported
   * @param containerId the ID of the container to be exported
   * @param outputStream the output stream to which the container will be exported
   * @param packer the tar container packer used for packing the container data
   * @throws IOException if an I/O error occurs during exporting
   */
  public void exportContainer(final ContainerType type, final long containerId, final OutputStream outputStream,
      final TarContainerPacker packer) throws IOException {
    handlers.get(type).exportContainer(containerSet.getContainer(containerId), outputStream, packer);
  }

  /**
   * Deletes a container given its id.
   * @param containerId Id of the container to be deleted
   * @param force if this is set to true, we delete container without checking
   * state of the container.
   */
  public void deleteContainer(final long containerId, boolean force) throws IOException {
    final Container container = containerSet.getContainer(containerId);
    if (container != null) {
      getHandler(container).deleteContainer(container, force);
    }
  }

  /**
   * Given a container, returns its handler instance.
   *
   * @param container Container
   * @return handler of the container
   */
  private Handler getHandler(final Container container) {
    return handlers.get(container.getContainerType());
  }

  /**
   * Retrieves an iterable collection of all containers managed by this controller.
   *
   * @return an iterable collection of containers
   */
  public Iterable<Container<?>> getContainers() {
    return containerSet;
  }

  /**
   * Return an iterator of containers which are associated with the specified <code>volume</code>.
   *
   * @param  volume the HDDS volume which should be used to filter containers
   * @return {@literal Iterator<Container>}
   */
  public Iterator<Container<?>> getContainers(HddsVolume volume) {
    return containerSet.getContainerIterator(volume);
  }

  /**
   * Get the number of containers based on the given volume.
   *
   * @param volume hdds volume.
   * @return number of containers.
   */
  public long getContainerCount(HddsVolume volume) {
    return containerSet.containerCount(volume);
  }

  void updateDataScanTimestamp(long containerId, Instant timestamp) throws IOException {
    Container container = containerSet.getContainer(containerId);
    if (container != null) {
      container.updateDataScanTimestamp(timestamp);
    } else {
      LOG.warn("Container {} not found, may be deleted, skip update DataScanTimestamp", containerId);
    }
  }
}
