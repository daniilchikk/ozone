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

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.VerifyBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerMultinodeApi;
import org.apache.hadoop.hdds.scm.storage.ContainerMultinodeApiImpl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;

/**
 * OzoneFsckHandler is responsible for checking the integrity of keys in an Ozone filesystem.
 * It traverses volumes, buckets, and keys to detect and optionally delete corrupted keys.
 */
public class OzoneFsckHandler implements AutoCloseable {
  private final OzoneAddress address;

  private final OzoneFsckVerboseSettings verboseSettings;

  private final Writer writer;

  private final boolean deleteCorruptedKeys;

  private final OzoneClient client;

  private final OzoneManagerProtocol omClient;

  private final ContainerOperationClient containerOperationClient;

  private final XceiverClientManager xceiverClientManager;

  public OzoneFsckHandler(OzoneAddress address, OzoneFsckVerboseSettings verboseSettings, Writer writer,
      boolean deleteCorruptedKeys, OzoneClient client, OzoneConfiguration ozoneConfiguration) throws IOException {
    this.address = address;
    this.verboseSettings = verboseSettings;
    this.writer = writer;
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
    Iterator<? extends OzoneVolume> volumes = client.getObjectStore().listVolumes(address.getVolumeName());

    while (volumes.hasNext()) {
      scanBuckets(volumes.next());
    }
  }

  private void scanBuckets(OzoneVolume volume) throws IOException {
    Iterator<? extends OzoneBucket> buckets = volume.listBuckets(address.getBucketName());

    while (buckets.hasNext()) {
      scanKeys(buckets.next());
    }
  }

  private void scanKeys(OzoneBucket bucket) throws IOException {
    Iterator<? extends OzoneKey> keys = bucket.listKeys(address.getKeyName());

    while (keys.hasNext()) {
      scanKey(keys.next());
    }
  }

  private void scanKey(OzoneKey key) throws IOException {
    OmKeyArgs keyArgs = createKeyArgs(key);

    KeyInfoWithVolumeContext keyInfoWithContext = omClient.getKeyInfo(keyArgs, false);

    OmKeyInfo keyInfo = keyInfoWithContext.getKeyInfo();

    List<OmKeyLocationInfo> locations = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();

    for (OmKeyLocationInfo location : locations) {
      Pipeline pipeline = getKeyPipeline(location.getPipeline());

      XceiverClientSpi xceiverClient = xceiverClientManager.acquireClientForReadData(pipeline);

      try (ContainerMultinodeApi containerClient = new ContainerMultinodeApiImpl(xceiverClient)) {
        Map<DatanodeDetails, VerifyBlockResponseProto> responses = containerClient.verifyBlock(
            location.getBlockID().getDatanodeBlockIDProtobuf(),
            location.getToken()
        );

        for (VerifyBlockResponseProto response : responses.values()) {
          if (response.hasValid() && !response.getValid()) {
            writer.write(String.format("Block %s is damaged", location.getBlockID()));
          }
        }
      } catch (Exception e) {
        throw new IOException("Can't sent request to Datanode.", e);
      }
    }
  }

  private Pipeline getKeyPipeline(Pipeline keyPipeline) {
    boolean isECKey = keyPipeline.getReplicationConfig().getReplicationType() == HddsProtos.ReplicationType.EC;
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
