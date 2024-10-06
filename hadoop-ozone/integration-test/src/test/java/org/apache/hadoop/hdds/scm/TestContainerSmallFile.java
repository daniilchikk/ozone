/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.EchoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManager;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManagerImpl;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.client.ContainerApi;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Container calls.
 */
@Timeout(300)
public class TestContainerSmallFile {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static ContainerApiManager containerApiManager;

  @BeforeAll
  public static void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    containerApiManager = new ContainerApiManagerImpl();
  }

  @AfterAll
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testEcho() throws Exception {
    ContainerWithPipeline container = storageContainerLocationClient.allocateContainer(
        SCMTestUtils.getReplicationType(ozoneConfig),
        HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);

    ContainerApi containerClient = containerApiManager.acquireClient(container.getPipeline());

    containerClient.createContainer(container.getContainerInfo().getContainerID());
    ByteString byteString = UnsafeByteOperations.unsafeWrap(new byte[0]);
    EchoResponseProto response =
        containerClient.echo(container.getContainerInfo().getContainerID(), byteString, 1, 0, true);
    assertEquals(1, response.getPayload().size());
  }
}


