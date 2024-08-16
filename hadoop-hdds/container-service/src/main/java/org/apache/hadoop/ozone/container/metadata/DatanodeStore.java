/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.metadata;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;

/**
 * Interface for interacting with datanode databases.
 */
public interface DatanodeStore extends Closeable {
  String NO_SUCH_BLOCK_ERR_MSG = "Unable to find the block.";

  /**
   * Start datanode manager.
   *
   * @param configuration - Configuration
   * @throws IOException - Unable to start datanode store.
   */
  void start(ConfigurationSource configuration) throws IOException;

  /**
   * Stop datanode manager.
   */
  void stop() throws Exception;

  /**
   * Get datanode store.
   *
   * @return datanode store.
   */
  @VisibleForTesting
  DBStore getStore();

  /**
   * A Table that keeps the block data.
   *
   * @return Table
   */
  Table<String, BlockData> getBlockDataTable();

  /**
   * A Table that keeps the metadata.
   *
   * @return Table
   */
  Table<String, Long> getMetadataTable();

  /**
   * A Table that keeps IDs of blocks deleted from the block data table.
   *
   * @return Table
   */
  Table<String, ChunkInfoList> getDeletedBlocksTable();

  /**
   * A Table that keeps finalize blocks requested from a client.
   *
   * @return Table
   */
  Table<String, Long> getFinalizeBlocksTable();

  /**
   * A Table that keeps the metadata of the last chunk of blocks.
   *
   * @return Table
   */
  Table<String, BlockData> getLastChunkInfoTable();

  /**
   * Helper to create and write batch transactions.
   */
  BatchOperationHandler getBatchHandler();

  /**
   * Flushes the log of the underlying database.
   *
   * @param sync If true, the flush operation will be synchronized to disk.
   * @throws IOException If an I/O error occurs during the flush.
   */
  void flushLog(boolean sync) throws IOException;

  /**
   * Flushes all pending changes to the underlying database.
   *
   * @throws IOException If an I/O error occurs during the flush operation.
   */
  void flushDB() throws IOException;

  /**
   * Compacts the underlying database to improve space utilization and performance.
   * This operation may be necessary to reclaim space from deleted blocks
   */
  void compactDB() throws IOException;

  /**
   * Retrieves a BlockIterator that iterates over block data for a specified container ID.
   *
   * @param containerID The ID of the container whose block data is to be iterated over.
   * @return {@link BlockIterator<BlockData>} An iterator over the blocks in the specified container.
   * @throws IOException If an I/O error occurs while retrieving the block iterator.
   */
  BlockIterator<BlockData> getBlockIterator(long containerID) throws IOException;

  /**
   * Retrieves a BlockIterator that iterates over block data for a specified container ID
   * and filtered by a given KeyPrefixFilter.
   *
   * @param containerID The ID of the container whose block data is to be iterated over.
   * @param filter The KeyPrefixFilter to apply to the block data iteration.
   * @return {@link BlockIterator<BlockData>} An iterator over the blocks in the specified container
   * that match the given filter.
   * @throws IOException If an I/O error occurs while retrieving the block iterator.
   */
  BlockIterator<BlockData> getBlockIterator(long containerID, KeyPrefixFilter filter) throws IOException;

  /**
   * Retrieves a BlockIterator that iterates over the final blocks for a specific container ID,
   * filtered by a given KeyPrefixFilter.
   *
   * @param containerID The ID of the container whose finalize blocks are to be iterated over.
   * @param filter The KeyPrefixFilter to apply to the final blocks' iteration.
   * @return {@link BlockIterator<Long>} An iterator over the finalized blocks in the specified container
   * that match the given filter.
   * @throws IOException If an I/O error occurs while retrieving the block iterator.
   */
  BlockIterator<Long> getFinalizeBlockIterator(long containerID, KeyPrefixFilter filter) throws IOException;

  /**
   * Returns if the underlying DB is closed. This call is thread safe.
   *
   * @return true if the DB is closed.
   */
  boolean isClosed();

  /**
   * Performs a compaction operation on the underlying database if needed.
   * This method checks if the database requires compaction to improve space utilization and performance.
   * Compaction may be necessary to reclaim space from deleted blocks or optimize the database structure.
   *
   * @throws Exception If an error occurs during the compaction process.
   */
  default void compactionIfNeeded() throws Exception {
  }

  /**
   * Retrieves the block data associated with the specified block key.
   *
   * @param blockID The ID of the block to retrieve.
   * @param blockKey The key associated with the block in the block data table.
   * @return BlockData The data of the specified block.
   * @throws IOException If an I/O error occurs, or the block data is not found.
   */
  default BlockData getBlockByID(BlockID blockID, String blockKey) throws IOException {
    // check block data table
    BlockData blockData = getBlockDataTable().get(blockKey);

    return getCompleteBlockData(blockData, blockID, blockKey);
  }

  default BlockData getCompleteBlockData(BlockData blockData,
      BlockID blockID, String blockKey) throws IOException {
    if (blockData == null) {
      throw new StorageContainerException(NO_SUCH_BLOCK_ERR_MSG + " BlockID : " + blockID, NO_SUCH_BLOCK);
    }

    return blockData;
  }

  /**
   * Puts a block by its local ID into the block data table within a batch operation.
   *
   * @param batch The batch operation in which to execute the put operation.
   * @param incremental A flag indicating if the operation is incremental.
   * @param localID The local ID of the block to be put.
   * @param data The block data to be stored.
   * @param containerData The container data associated with the block.
   * @param endOfBlock A flag indicating if this is the end of block.
   * @throws IOException If an I/O error occurs during the operation.
   */
  default void putBlockByID(BatchOperation batch, boolean incremental, long localID, BlockData data,
      KeyValueContainerData containerData, boolean endOfBlock) throws IOException {
    // old client: override a chunk list.
    getBlockDataTable().putWithBatch(batch, containerData.getBlockKey(localID), data);
  }
}
