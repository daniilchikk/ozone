package org.apache.hadoop.ozone.container.keyvalue.interfaces;

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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.ratis.statemachine.StateMachine;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Chunk Manager allows read, write, delete and listing of chunks in a container.
 */
public interface ChunkManager {

  /**
   * Writes a chunk of data to the specified container and block.
   *
   * @param container The container to which the chunk is written.
   * @param blockID The ID of the block within the container.
   * @param info Metadata about the chunk being written.
   * @param data Buffer containing the chunk data to be written.
   * @param dispatcherContext Context information for the dispatcher performing the write operation.
   * @throws StorageContainerException If an error occurs during the write operation.
   */
  void writeChunk(Container container, BlockID blockID, ChunkInfo info, ChunkBuffer data,
      DispatcherContext dispatcherContext) throws StorageContainerException;

  /**
   * Writes a chunk of data to the specified container and block.
   *
   * @param container The container to which the chunk is written.
   * @param blockID The ID of the block within the container.
   * @param info Metadata about the chunk being written.
   * @param data ByteBuffer containing the chunk data to be written.
   * @param dispatcherContext Context information for the dispatcher performing the write operation.
   * @throws StorageContainerException If an error occurs during the write operation.
   */
  default void writeChunk(Container container, BlockID blockID, ChunkInfo info, ByteBuffer data,
      DispatcherContext dispatcherContext) throws StorageContainerException {
    ChunkBuffer wrapper = ChunkBuffer.wrap(data);
    writeChunk(container, blockID, info, wrapper, dispatcherContext);
  }

  /**
   * Reads the data defined by a chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info - ChunkInfo.
   * @param dispatcherContext - dispatcher context info.
   * @return  byte array
   * @throws StorageContainerException
   *
   * TODO: Right now we do not support partial reads and writes of chunks.
   * TODO: Explore if we need to do that for ozone.
   */
  ChunkBuffer readChunk(Container container, BlockID blockID, ChunkInfo info, DispatcherContext dispatcherContext)
      throws StorageContainerException;

  /**
   * Deletes a given chunk.
   *
   * @param container - Container for the chunk
   * @param blockID - ID of the block.
   * @param info  - Chunk Info
   */
  void deleteChunk(Container container, BlockID blockID, ChunkInfo info) throws StorageContainerException;

  /**
   * Deletes the chunks associated with the given block data in the specified container.
   *
   * @param container The container from which the chunks will be deleted.
   * @param blockData The block data containing information about the chunks to be deleted.
   * @throws StorageContainerException If an error occurs during the deletion process.
   */
  void deleteChunks(Container container, BlockData blockData) throws StorageContainerException;

  // TODO : Support list operations.

  /**
   * Shutdown the chunkManager.
   */
  default void shutdown() {
    // if applicable
  }

  /**
   * Finalizes the process of writing chunks to the specified container and block data.
   *
   * @param kvContainer The container to which the chunks are being written.
   * @param blockData The block data associated with the chunks being written.
   * @throws IOException If an I/O error occurs during the finalization process.
   */
  default void finishWriteChunks(KeyValueContainer kvContainer, BlockData blockData) throws IOException {
    // no-op
  }

  /**
   * Finalizes the process of writing a chunk to the specified container and block.
   *
   * @param container The container where the chunk is written.
   * @param blockId The ID of the block within the container.
   * @throws IOException If an I/O error occurs during the finalization process.
   */
  default void finalizeWriteChunk(KeyValueContainer container, BlockID blockId) throws IOException {
    // no-op
  }

  /**
   * Initializes a data stream for the specified container and block.
   *
   * @param container The container where the stream will be initialized.
   * @param blockID The ID of the block within the container.
   * @return A string that represents the status or identifier of the initialized stream.
   * @throws StorageContainerException If an error occurs during the stream initialization.
   */
  default String streamInit(Container container, BlockID blockID) throws StorageContainerException {
    return null;
  }

  /**
   * Retrieves the data channel for streaming data within a specified container and block.
   *
   * @param container The container where the data stream exists.
   * @param blockID The ID of the block within the container.
   * @param metrics Metrics pertaining to the container.
   * @return The DataChannel object associated with the specified container and block.
   * @throws StorageContainerException If an error occurs during the execution of the method.
   */
  default StateMachine.DataChannel getStreamDataChannel(Container container, BlockID blockID, ContainerMetrics metrics)
      throws StorageContainerException {
    return null;
  }

  /**
   * Determines the appropriate buffer capacity for reading a chunk based on provided chunk information
   * and a default buffer capacity.
   *
   * @param chunkInfo The metadata information about the chunk to be read.
   * @param defaultReadBufferCapacity The default buffer capacity to be used if no specific capacity is determined.
   * @return The calculated buffer capacity for reading the chunk.
   */
  static int getBufferCapacityForChunkRead(ChunkInfo chunkInfo, int defaultReadBufferCapacity) {
    long bufferCapacity = 0;
    if (chunkInfo.isReadDataIntoSingleBuffer()) {
      // Older client - read all chunk data into one single buffer.
      bufferCapacity = chunkInfo.getLen();
    } else {
      // Set buffer capacity to checksum boundary size so that each buffer corresponds to one checksum.
      // If checksum is NONE, then set buffer capacity to default (OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY = 1MB).
      ChecksumData checksumData = chunkInfo.getChecksumData();

      if (checksumData != null) {
        if (checksumData.getChecksumType() == ContainerProtos.ChecksumType.NONE) {
          bufferCapacity = defaultReadBufferCapacity;
        } else {
          bufferCapacity = checksumData.getBytesPerChecksum();
        }
      }
    }
    // If the buffer capacity is 0, set all the data into one ByteBuffer
    if (bufferCapacity == 0) {
      bufferCapacity = chunkInfo.getLen();
    }

    if (bufferCapacity > Integer.MAX_VALUE) {
      throw new IllegalStateException("Integer overflow:"
          + " bufferCapacity = " + bufferCapacity
          + " > Integer.MAX_VALUE = " + Integer.MAX_VALUE
          + ", defaultReadBufferCapacity=" + defaultReadBufferCapacity
          + ", chunkInfo=" + chunkInfo);
    }
    return Math.toIntExact(bufferCapacity);
  }
}
