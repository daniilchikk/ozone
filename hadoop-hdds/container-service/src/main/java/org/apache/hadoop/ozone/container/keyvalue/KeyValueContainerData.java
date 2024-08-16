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

package org.apache.hadoop.ozone.container.keyvalue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.yaml.snakeyaml.nodes.Tag;


import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;
import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE_ROCKSDB;
import static org.apache.hadoop.ozone.OzoneConsts.CHUNKS_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.DELETE_TRANSACTION_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.DELETING_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_VERSION;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_BYTES_USED;
import static org.apache.hadoop.ozone.OzoneConsts.BLOCK_COUNT;
import static org.apache.hadoop.ozone.OzoneConsts.PENDING_DELETE_BLOCK_COUNT;
import static org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix;

/**
 * This class represents the KeyValueContainer metadata,
 * which is the in-memory representation of container metadata and is represented on disk by the .container file.
 */
public class KeyValueContainerData extends ContainerData {

  // Yaml Tag used for KeyValueContainerData.
  public static final Tag KEYVALUE_YAML_TAG = new Tag("KeyValueContainerData");

  // Fields need to be stored in .container file.
  private static final List<String> KV_YAML_FIELDS;

  // Path to Container metadata Level DB/RocksDB Store and .container file.
  private String metadataPath;

  //Type of DB used to store key to chunk mapping
  private String containerDBType = CONTAINER_DB_TYPE_ROCKSDB;

  private File dbFile = null;

  private String schemaVersion;

  /**
   * Number of pending deletion blocks in KeyValueContainer.
   */
  private final AtomicLong numPendingDeletionBlocks;

  private long deleteTransactionId;

  private long blockCommitSequenceId;

  private final Set<Long> finalizedBlockSet;

  static {
    // Initialize YAML fields
    KV_YAML_FIELDS = Lists.newArrayList();
    KV_YAML_FIELDS.addAll(YAML_FIELDS);
    KV_YAML_FIELDS.add(METADATA_PATH);
    KV_YAML_FIELDS.add(CHUNKS_PATH);
    KV_YAML_FIELDS.add(CONTAINER_DB_TYPE);
    KV_YAML_FIELDS.add(SCHEMA_VERSION);
  }

  /**
   * Constructs KeyValueContainerData object.
   *
   * @param id - ContainerId
   * @param layoutVersion container layout
   * @param size - maximum size of the container in bytes
   */
  public KeyValueContainerData(long id, ContainerLayoutVersion layoutVersion, long size, String originPipelineId,
      String originNodeId) {
    super(ContainerProtos.ContainerType.KeyValueContainer, id, layoutVersion, size, originPipelineId, originNodeId);
    this.numPendingDeletionBlocks = new AtomicLong(0);
    this.deleteTransactionId = 0;
    finalizedBlockSet =  ConcurrentHashMap.newKeySet();
  }

  public KeyValueContainerData(KeyValueContainerData source) {
    super(source);
    Preconditions.checkArgument(source.getContainerType() == ContainerProtos.ContainerType.KeyValueContainer);
    this.numPendingDeletionBlocks = new AtomicLong(0);
    this.deleteTransactionId = 0;
    this.schemaVersion = source.getSchemaVersion();
    finalizedBlockSet = ConcurrentHashMap.newKeySet();
  }

  /**
   * @param version The schema version indicating the table layout of the container's database.
   */
  public void setSchemaVersion(String version) {
    schemaVersion = version;
  }

  /**
   * @return The schema version describing the container database's table layout.
   */
  public String getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * Returns schema version or the default value when the {@link KeyValueContainerData#schemaVersion} is null.
   * The default value can be referred to {@link KeyValueContainerUtil#isSameSchemaVersion}.
   *
   * @return Schema version as a string.
   * @throws UnsupportedOperationException If no valid schema version is found.
   */
  public String getSupportedSchemaVersionOrDefault() {
    String[] versions = {SCHEMA_V1, SCHEMA_V2, SCHEMA_V3};

    for (String version : versions) {
      if (this.hasSchema(version)) {
        return version;
      }
    }
    throw new UnsupportedOperationException("No valid schema version found.");
  }

  /**
   * Sets Container dbFile. This should be called only during the creation of KeyValue container.
   */
  public void setDbFile(File containerDbFile) {
    dbFile = containerDbFile;
  }

  /**
   * Returns container DB file.
   *
   * @return dbFile
   */
  public File getDbFile() {
    return dbFile;
  }

  /**
   * Returns container metadata path.
   *
   * @return - Physical path where container file and checksum are stored.
   */
  public String getMetadataPath() {
    return metadataPath;
  }

  /**
   * Sets container metadata path.
   *
   * @param path - String.
   */
  public void setMetadataPath(String path) {
    this.metadataPath = path;
  }

  /**
   * Returns the path to base dir of the container.
   *
   * @return Path to base dir
   */
  @Override
  public String getContainerPath() {
    return new File(metadataPath).getParent();
  }

  /**
   * Returns the blockCommitSequenceId.
   */
  @Override
  public long getBlockCommitSequenceId() {
    return blockCommitSequenceId;
  }

  /**
   * Updates the blockCommitSequenceId.
   */
  public void updateBlockCommitSequenceId(long id) {
    this.blockCommitSequenceId = id;
  }

  /**
   * Returns the DBType used for the container.
   *
   * @return containerDBType
   */
  public String getContainerDBType() {
    return containerDBType;
  }

  /**
   * Sets the DBType used for the container.
   */
  public void setContainerDBType(String containerDBType) {
    this.containerDBType = containerDBType;
  }

  /**
   * Increase the count of pending deletion blocks.
   *
   * @param numBlocks increment number
   */
  public void incrPendingDeletionBlocks(long numBlocks) {
    this.numPendingDeletionBlocks.addAndGet(numBlocks);
  }

  /**
   * Decrease the count of pending deletion blocks.
   *
   * @param numBlocks decrement number
   */
  public void decrPendingDeletionBlocks(long numBlocks) {
    this.numPendingDeletionBlocks.addAndGet(-1 * numBlocks);
  }

  /**
   * Get the number of pending deletion blocks.
   */
  public long getNumPendingDeletionBlocks() {
    return this.numPendingDeletionBlocks.get();
  }

  /**
   * Sets deleteTransactionId to latest delete transactionId for the container.
   *
   * @param transactionId latest transactionId of the container.
   */
  public void updateDeleteTransactionId(long transactionId) {
    deleteTransactionId = max(transactionId, deleteTransactionId);
  }

  /**
   * Return the latest deleteTransactionId of the container.
   */
  public long getDeleteTransactionId() {
    return deleteTransactionId;
  }

  /**
   * Add the given localID of a block to the finalizedBlockSet.
   */
  public void addToFinalizedBlockSet(long localID) {
    finalizedBlockSet.add(localID);
  }

  /**
   * Returns a set of finalized block IDs associated with the container data.
   *
   * @return a Set of Long values representing the IDs of finalized blocks.
   */
  public Set<Long> getFinalizedBlockSet() {
    return finalizedBlockSet;
  }

  /**
   * Checks if a block with the specified local ID exists in the finalized block set.
   *
   * @param localID The ID of the block to check for existence in the finalized block set.
   * @return true if the block exists in the finalized block set, false otherwise.
   */
  public boolean isFinalizedBlockExist(long localID) {
    return finalizedBlockSet.contains(localID);
  }

  /**
   * Clears the set of finalized blocks from both memory and the database.
   * This operation will remove all finalized blocks associated with the current container's prefix.
   * It first checks if the finalized block set is not empty,
   * then deletes the corresponding entries from the database using batch operations and clears the in-memory set.
   *
   * @param db The database handle to use it for the batch operations. It must not be {@code null}.
   * @throws IOException If any I/O error occurs during the batch operations.
   */
  public void clearFinalizedBlock(DBHandle db) throws IOException {
    if (!finalizedBlockSet.isEmpty()) {
      // Delete it from db and clear memory.
      // Should never fail.
      Preconditions.checkNotNull(db, "DB cannot be null here");
      try (BatchOperation batch = db.getStore().getBatchHandler().initBatchOperation()) {
        db.getStore().getFinalizeBlocksTable().deleteBatchWithPrefix(batch, containerPrefix());
        db.getStore().getBatchHandler().commitBatchOperation(batch);
      }
      finalizedBlockSet.clear();
    }
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  @Override
  @JsonIgnore
  public ContainerDataProto getProtoBufMessage() {
    ContainerDataProto.Builder builder = ContainerDataProto.newBuilder();
    builder.setContainerID(this.getContainerID());
    builder.setContainerPath(this.getContainerPath());
    builder.setState(this.getState());
    builder.setBlockCount(this.getBlockCount());

    for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder = ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey()).setValue(entry.getValue()).build());
    }

    if (this.getBytesUsed() >= 0) {
      builder.setBytesUsed(this.getBytesUsed());
    }

    if (this.getContainerType() != null) {
      builder.setContainerType(ContainerProtos.ContainerType.KeyValueContainer);
    }

    return builder.build();
  }

  /**
   * Returns an unmodifiable list of YAML field names used in the key-value container.
   *
   * @return a List of Strings representing the YAML field names.
   */
  public static List<String> getYamlFields() {
    return Collections.unmodifiableList(KV_YAML_FIELDS);
  }

  /**
   * Update DB counters related to block metadata.
   *
   * @param db - Reference to container DB.
   * @param batchOperation - Batch Operation to batch DB operations.
   * @param deletedBlockCount - Number of blocks deleted.
   * @param releasedBytes - Number of bytes released.
   */
  public void updateAndCommitDBCounters(DBHandle db, BatchOperation batchOperation, int deletedBlockCount,
      long releasedBytes) throws IOException {
    Table<String, Long> metadataTable = db.getStore().getMetadataTable();

    // Set Bytes used and block count key.
    metadataTable.putWithBatch(batchOperation, getBytesUsedKey(), getBytesUsed() - releasedBytes);
    metadataTable.putWithBatch(batchOperation, getBlockCountKey(), getBlockCount() - deletedBlockCount);
    metadataTable.putWithBatch(
        batchOperation,
        getPendingDeleteBlockCountKey(),
        getNumPendingDeletionBlocks() - deletedBlockCount);

    db.getStore().getBatchHandler().commitBatchOperation(batchOperation);
  }

  /**
   * Resets the count of pending deletion blocks to zero.
   *
   * @param db The database handle used to access the container's metadata table.
   * @throws IOException If an I/O error occurs while updating the metadata table on disk.
   */
  public void resetPendingDeleteBlockCount(DBHandle db) throws IOException {
    // Reset the in memory metadata.
    numPendingDeletionBlocks.set(0);
    // Reset the metadata on disk.
    Table<String, Long> metadataTable = db.getStore().getMetadataTable();
    metadataTable.put(getPendingDeleteBlockCountKey(), 0L);
  }

  // NOTE: Below are some helper functions to format keys according to container schemas,
  // we should use them instead of using raw const variables defined.

  /**
   * Generates a formatted key for accessing block data using the provided local block ID.
   *
   * @param localID The local ID of the block within the container.
   * @return The formatted key string specific to the container's schema.
   */
  public String getBlockKey(long localID) {
    return formatKey(Long.toString(localID));
  }

  /**
   * Generates a deleting block key string using the provided local ID.
   *
   * @param localID The local ID of the block within the container.
   * @return The formatted key string specific to the container's schema with the deleting block key prefix.
   */
  public String getDeletingBlockKey(long localID) {
    return formatKey(DELETING_KEY_PREFIX + localID);
  }

  /**
   * Generates a formatted key for accessing delete transaction data using the provided transaction ID.
   *
   * @param txnID The ID of the transaction to be deleted.
   * @return The formatted key string specific to the container's schema.
   */
  public String getDeleteTxnKey(long txnID) {
    return formatKey(Long.toString(txnID));
  }

  /**
   * Retrieves the formatted key for the latest delete transaction.
   *
   * @return A String representing the formatted key for the latest delete transaction.
   */
  public String getLatestDeleteTxnKey() {
    return formatKey(DELETE_TRANSACTION_KEY);
  }

  /**
   * Retrieves the formatted key specific to the Block Commit Sequence ID.
   *
   * @return A String representing the formatted key for the Block Commit Sequence ID.
   */
  public String getBcsIdKey() {
    return formatKey(BLOCK_COMMIT_SEQUENCE_ID);
  }

  /**
   * Retrieves the formatted key specific to the Block Count.
   *
   * @return A String representing the formatted key for the Block Count.
   */
  public String getBlockCountKey() {
    return formatKey(BLOCK_COUNT);
  }

  /**
   * Retrieves the formatted key specific to the bytes used in the container.
   * This key is used to store or query the bytes used information from the database specific to the container's schema.
   *
   * @return A String representing the formatted key for bytes used.
   */
  public String getBytesUsedKey() {
    return formatKey(CONTAINER_BYTES_USED);
  }

  /**
   * Retrieves the formatted key specific to the Pending Delete Block Count.
   *
   * @return A String representing the formatted key for the Pending Delete Block Count.
   */
  public String getPendingDeleteBlockCountKey() {
    return formatKey(PENDING_DELETE_BLOCK_COUNT);
  }

  /**
   * Retrieves the key prefix used for deleting blocks within the container.
   *
   * @return A String representing the formatted key prefix specific to the container's schema for deleting blocks.
   */
  public String getDeletingBlockKeyPrefix() {
    return formatKey(DELETING_KEY_PREFIX);
  }

  /**
   * Returns a KeyPrefixFilter that is configured to filter out keys with the container's schema-specific prefix.
   *
   * @return a KeyPrefixFilter object that filters out keys using the container's schema-specific prefix.
   */
  public KeyPrefixFilter getUnprefixedKeyFilter() {
    String schemaPrefix = containerPrefix();
    return new KeyPrefixFilter().addFilter(schemaPrefix + "#", true);
  }

  /**
   * Generates and returns a {@link KeyPrefixFilter}
   * configured to filter out keys that have the prefix used for deleting blocks within the container.
   *
   * @return a KeyPrefixFilter object configured to filter keys with the deleting block key prefix.
   */
  public KeyPrefixFilter getDeletingBlockKeyFilter() {
    return new KeyPrefixFilter().addFilter(getDeletingBlockKeyPrefix());
  }

  /**
   * Schema v3 use a prefix as startKey, for other schemas return {@code null}.
   */
  public String startKeyEmpty() {
    if (hasSchema(SCHEMA_V3)) {
      return getContainerKeyPrefix(getContainerID());
    }
    return null;
  }

  /**
   * Schema v3 use containerID as key prefix, for other schemas {@code null}.
   */
  public String containerPrefix() {
    if (hasSchema(SCHEMA_V3)) {
      return getContainerKeyPrefix(getContainerID());
    }
    return "";
  }

  /**
   * Format the raw key to a schema-specific format key.
   * Schema v3 uses container ID as key prefix, for other schemas return the raw key.
   *
   * @param key raw key
   * @return formatted key
   */
  private String formatKey(String key) {
    if (hasSchema(SCHEMA_V3)) {
      key = getContainerKeyPrefix(getContainerID()) + key;
    }
    return key;
  }

  /**
   * Checks if the provided version matches the schema version of the container.
   *
   * @param version The schema version to compare against the container's schema version.
   * @return true if the provided version matches the container's schema version, false otherwise.
   */
  public boolean hasSchema(String version) {
    return KeyValueContainerUtil.isSameSchemaVersion(schemaVersion, version);
  }
}
