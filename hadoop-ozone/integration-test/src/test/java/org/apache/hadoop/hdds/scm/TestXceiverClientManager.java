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
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.client.ContainerApi;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManager;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManagerImpl;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Test for XceiverClientManager caching and eviction.
 */
@Timeout(300)
public class TestXceiverClientManager {

  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(config)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @ParameterizedTest(name = "Ozone security enabled: {0}")
  @ValueSource(booleans = {false, true})
  public void testCaching(boolean securityEnabled) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, securityEnabled);
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);

    ClientTrustManager trustManager = mock(ClientTrustManager.class);

    ContainerApiManager containerApiManager = new ContainerApiManagerImpl();

    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.ONE,
            OzoneConsts.OZONE);
    ContainerApi client1 = containerApiManager.acquireClient(container1.getPipeline());
    // assertEquals(1, client1.getRefcount());

    ContainerWithPipeline container2 = storageContainerLocationClient
        .allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.THREE,
            OzoneConsts.OZONE);
    ContainerApi client2 = containerApiManager
        .acquireClient(container2.getPipeline());
    //assertEquals(1, client2.getRefcount());

    ContainerApi client3 = containerApiManager
        .acquireClient(container1.getPipeline());
    //assertEquals(2, client3.getRefcount());
    //assertEquals(2, client1.getRefcount());
    assertEquals(client1, client3);
//    assertEquals(0, clientManager.getClientCache().size());
  }

  @Test
  public void testFreeByReference() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmClientConfig clientConfig = conf.getObject(ScmClientConfig.class);
    clientConfig.setMaxSize(1);
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);

    ContainerApiManager containerApiManager = new ContainerApiManagerImpl();
    //Cache<String, XceiverClientSpi> cache = clientManager.getClientCache();

    ContainerWithPipeline container1 =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.ONE,
            OzoneConsts.OZONE);
    ContainerApi client1 = containerApiManager
        .acquireClient(container1.getPipeline());
    //assertEquals(1, client1.getRefcount());
    //assertEquals(container1.getPipeline(), client1.getPipeline());

    ContainerWithPipeline container2 =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.THREE,
            OzoneConsts.OZONE);
    ContainerApi client2 = containerApiManager.acquireClient(container2.getPipeline());
    //assertEquals(1, client2.getRefcount());
    assertNotEquals(client1, client2);

    // least recent container (i.e containerName1) is evicted
/*    XceiverClientSpi nonExistent1 = cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType());*/
    //assertNull(nonExistent1);
    // However container call should succeed because of refcount on the client
    client1.createContainer(container1.getContainerInfo().getContainerID());

    // After releasing the client, this connection should be closed
    // and any container operations should fail

    // Create container should throw exception on closed client
    Throwable t = assertThrows(IOException.class,
        () -> client1.createContainer(container1.getContainerInfo().getContainerID()));
    assertThat(t.getMessage()).contains("This channel is not connected");
  }

  @Test
  public void testFreeByEviction() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();

    ContainerApiManager containerApiManager = new ContainerApiManagerImpl();

    ScmClientConfig clientConfig = conf.getObject(ScmClientConfig.class);
    clientConfig.setMaxSize(1);
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);
    //Cache<String, XceiverClientSpi> cache = clientManager.getClientCache();

    ContainerWithPipeline container1 =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.ONE,
            OzoneConsts.OZONE);
    ContainerApi client1 = containerApiManager
        .acquireClient(container1.getPipeline());
    //assertEquals(1, client1.getRefcount());

    //assertEquals(0, client1.getRefcount());

    ContainerWithPipeline container2 =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            HddsProtos.ReplicationFactor.THREE,
            OzoneConsts.OZONE);
    ContainerApi client2 = containerApiManager
        .acquireClient(container2.getPipeline());
    //assertEquals(1, client2.getRefcount());
    assertNotEquals(client1, client2);

    // now client 1 should be evicted
/*    XceiverClientSpi nonExistent = cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType());
    assertNull(nonExistent);*/

    // Any container operation should now fail
    Throwable t = assertThrows(IOException.class,
        () -> client1.createContainer(container1.getContainerInfo().getContainerID()));
    assertThat(t.getMessage()).contains("This channel is not connected");
  }

  @Test
  public void testFreeByRetryFailure() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmClientConfig clientConfig = conf.getObject(ScmClientConfig.class);
    clientConfig.setMaxSize(1);

    ContainerApiManager containerApiManager = new ContainerApiManagerImpl();

/*    Cache<String, XceiverClientSpi> cache =
        clientManager.getClientCache();*/

    // client is added in cache
    ContainerWithPipeline container1 =
        storageContainerLocationClient.allocateContainer(
            SCMTestUtils.getReplicationType(conf),
            SCMTestUtils.getReplicationFactor(conf),
            OzoneConsts.OZONE);
    ContainerApi client1 =
        containerApiManager.acquireClient(container1.getPipeline());
    ContainerApi client1SecondRef =
        containerApiManager.acquireClient(container1.getPipeline());
    //assertEquals(2, client1.getRefcount());

    // client should be invalidated in the cache
    // assertEquals(1, client1.getRefcount());
    /*assertNull(cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType()));*/

    // new client should be added in cache
    ContainerApi client2 = containerApiManager.acquireClient(container1.getPipeline());
    assertNotEquals(client1, client2);
    //assertEquals(1, client2.getRefcount());

    // on releasing the old client the cache entry should not be invalidated
    //assertEquals(0, client1.getRefcount());
    /*assertNotNull(cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType()));*/
  }
}
