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

package org.apache.hadoop.ozone.shell.fsck;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerMultinodeApi;
import org.apache.hadoop.hdds.scm.storage.ContainerMultinodeApiImpl;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.fsck.writer.OzoneFsckWriter;
import org.apache.hadoop.security.token.Token;

/**
 * OzoneFsckHandler is responsible for checking the integrity of keys in an Ozone filesystem.
 * It traverses volumes, buckets, and keys to detect and optionally to delete corrupted keys.
 */
public class OzoneFsckHandler implements AutoCloseable {

  private final OzoneFsckPathPrefix pathPrefix;

  private final OzoneFsckWriter writer;

  private final OzoneFsckVerboseSettings verboseSettings;

  private final boolean deleteCorruptedKeys;

  private final OzoneClient client;

  private final OzoneManagerProtocol omClient;

  private final ContainerOperationClient containerOperationClient;

  private final XceiverClientManager xceiverClientManager;

  /**
   * Constructs an OzoneFsckHandler that is used
   * to perform the Ozone File System Check (fscheck) operation in the Ozone filesystem.
   *
   * @throws IOException thrown in case {@link XceiverClientManager} instance can't be acquired
   * or {@link ContainerOperationClient} can't be instantiated with provided Ozone configuration.
   */
  public OzoneFsckHandler(
      OzoneFsckPathPrefix pathPrefix,
      OzoneFsckWriter writer,
      OzoneFsckVerboseSettings verboseSettings,
      boolean deleteCorruptedKeys,
      OzoneClient client,
      OzoneConfiguration ozoneConfiguration
  ) throws IOException {
    this.pathPrefix = pathPrefix;
    this.writer = writer;
    this.verboseSettings = verboseSettings;
    this.deleteCorruptedKeys = deleteCorruptedKeys;
    this.client = client;
    this.omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();
    this.containerOperationClient = new ContainerOperationClient(ozoneConfiguration);
    this.xceiverClientManager = containerOperationClient.getXceiverClientManager();
  }

  /**
   * Initiates a scan operation that traverses through the volumes, buckets, and keys to perform file system checks.
   *
   * @throws IOException if an I/O error occurs during the scan process.
   */
  public void scan() throws IOException {
    scanVolumes();
  }

  private void scanVolumes() throws IOException {
    Iterator<? extends OzoneVolume> volumes = client.getObjectStore().listVolumes(pathPrefix.volume());

    while (volumes.hasNext()) {
      scanBuckets(volumes.next());
    }
  }

  private void scanBuckets(OzoneVolume volume) throws IOException {
    Iterator<? extends OzoneBucket> buckets = volume.listBuckets(pathPrefix.container());

    while (buckets.hasNext()) {
      scanKeys(buckets.next());
    }
  }

  private void scanKeys(OzoneBucket bucket) throws IOException {
    Iterator<? extends OzoneKey> keys = bucket.listKeys(pathPrefix.key());

    while (keys.hasNext()) {
      scanKey(keys.next());
    }
  }

  private void scanKey(OzoneKey key) throws IOException {
    OmKeyArgs keyArgs = createKeyArgs(key);

    KeyInfoWithVolumeContext keyInfoWithContext = omClient.getKeyInfo(keyArgs, false);

    OmKeyInfo keyInfo = keyInfoWithContext.getKeyInfo();

    OmKeyLocationInfoGroup latestKeyInfo = keyInfo.getLatestVersionLocations();

    if (latestKeyInfo == null) {
      writer.writeCorruptedKey(keyInfo);
      return;
    }

    List<OmKeyLocationInfo> locations = latestKeyInfo.getBlocksLatestVersionOnly();

    Set<BlockID> damagedBlocks = new HashSet<>();

    for (OmKeyLocationInfo location : locations) {
      Pipeline pipeline = getKeyPipeline(location.getPipeline());

      XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);

      BlockID blockID = location.getBlockID().getProtobuf();

      try (ContainerMultinodeApi containerClient = new ContainerMultinodeApiImpl(xceiverClient)) {
        Map<DatanodeDetails, VerifyBlockResponseProto> responses = containerClient.verifyBlock(
            location.getBlockID().getDatanodeBlockIDProtobuf(),
            location.getToken()
        );

        if (responses.isEmpty()) {
          damagedBlocks.add(blockID);
        } else {
          for (VerifyBlockResponseProto response : responses.values()) {
            if (response.hasValid() && !response.getValid()) {
              damagedBlocks.add(blockID);
              break;
            }
          }

          if (!damagedBlocks.contains(blockID) && verboseSettings.printHealthyKeys()) {
            printKeyInformation(keyInfo, Collections.emptySet(), xceiverClient);
          }
        }

        if (!damagedBlocks.isEmpty()) {
          printKeyInformation(keyInfo, damagedBlocks, xceiverClient);

          if (deleteCorruptedKeys) {
            omClient.deleteKey(keyArgs);
          }
        }
      } catch (Exception e) {
        throw new IOException("Can't sent request to Datanode.", e);
      } finally {
        xceiverClientManager.releaseClientForReadData(xceiverClient, false);
      }
    }
  }

  private void printKeyInformation(OmKeyInfo keyInfo, Set<BlockID> damagedBlocks, XceiverClientSpi xceiverClient)
      throws IOException {
    boolean healthyKey = damagedBlocks.isEmpty();
    if (!healthyKey || verboseSettings.printHealthyKeys()) {
      writer.writeKeyInfo(keyInfo, () -> {
        writer.writeDamagedBlocks(damagedBlocks);

        printKeyDetails(keyInfo, xceiverClient);
      });
    }
  }

  private void printKeyDetails(OmKeyInfo keyInfo, XceiverClientSpi xceiverClient) throws IOException {
    if (verboseSettings.printContainers()) {
      OmKeyLocationInfoGroup locationInfoGroup = Objects.requireNonNull(keyInfo.getLatestVersionLocations());

      for (OmKeyLocationInfo locationInfo : locationInfoGroup.getBlocksLatestVersionOnly()) {
        Pipeline pipeline = locationInfo.getPipeline();

        if (pipeline.getType() != ReplicationType.STAND_ALONE && pipeline.getType() != ReplicationType.EC) {
          pipeline = Pipeline.newBuilder(pipeline)
              .setReplicationConfig(
                  StandaloneReplicationConfig.getInstance(
                      ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig())))
              .build();
        }

        writer.writeLocationInfo(locationInfo);

        Map<DatanodeDetails, ReadContainerResponseProto> readContainerResponses =
            readContainerInfos(locationInfo, pipeline);

        for (Map.Entry<DatanodeDetails, ReadContainerResponseProto> entry : readContainerResponses.entrySet()) {
          ContainerDataProto containerInfo = entry.getValue().getContainerData();
          DatanodeDetails datanodeDetails = entry.getKey();

          writer.writeContainerInfo(
              containerInfo,
              datanodeDetails,
              () -> printContainerDetails(locationInfo, xceiverClient)
          );
        }
      }
    }
  }

  private Map<DatanodeDetails, ReadContainerResponseProto> readContainerInfos(OmKeyLocationInfo locationInfo,
      Pipeline pipeline) throws IOException {
    try {
      return containerOperationClient.readContainerFromAllNodes(locationInfo.getContainerID(), pipeline);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void printContainerDetails(OmKeyLocationInfo locationInfo, XceiverClientSpi xceiverClient)
      throws IOException {
    if (verboseSettings.printBlocks()) {
      BlockData blockInfo = getChunksForLocation(locationInfo, xceiverClient);

      writer.writeBlockInfo(blockInfo, () -> printBlockDetails(blockInfo));
    }
  }

  private void printBlockDetails(BlockData blockInfo) throws IOException {
    if (verboseSettings.printChunks()) {
      List<ChunkInfo> chunkList = blockInfo.getChunksList();

      writer.writeChunkInfo(chunkList);
    }
  }

  private BlockData getChunksForLocation(OmKeyLocationInfo keyLocationInfo, XceiverClientSpi xceiverClient)
      throws IOException {
    Token<OzoneBlockTokenIdentifier> token = keyLocationInfo.getToken();
    Pipeline pipeline = keyLocationInfo.getPipeline();

    if (pipeline.getType() != ReplicationType.STAND_ALONE && pipeline.getType() != ReplicationType.EC) {
      pipeline = Pipeline.newBuilder(pipeline)
          .setReplicationConfig(StandaloneReplicationConfig
              .getInstance(ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig())))
          .build();
    }

    GetBlockResponseProto response = ContainerProtocolCalls.getBlock(
        xceiverClient,
        keyLocationInfo.getBlockID(),
        token,
        pipeline.getReplicaIndexes()
    );

    return response.getBlockData();
  }

  private Pipeline getKeyPipeline(Pipeline keyPipeline) {
    boolean isECKey = keyPipeline.getReplicationConfig().getReplicationType() == ReplicationType.EC;
    if (!isECKey && keyPipeline.getType() != STAND_ALONE) {
      return Pipeline.newBuilder(keyPipeline)
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
          .build();
    } else {
      return keyPipeline;
    }
  }

  private OmKeyArgs createKeyArgs(OzoneKey key) {
    return new OmKeyArgs.Builder()
        .setVolumeName(key.getVolumeName())
        .setBucketName(key.getBucketName())
        .setKeyName(key.getName())
        .build();
  }

  @Override
  public void close() throws Exception {
    this.xceiverClientManager.close();
    this.containerOperationClient.close();
  }
}
