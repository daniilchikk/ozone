 /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

 import org.apache.commons.lang3.RandomStringUtils;
 import org.apache.commons.lang3.RandomUtils;
 import org.apache.hadoop.hdds.client.BlockID;
 import org.apache.hadoop.hdds.conf.OzoneConfiguration;
 import org.apache.hadoop.hdds.protocol.DatanodeDetails;
 import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
 import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
 import org.apache.hadoop.hdds.scm.PlacementPolicy;
 import org.apache.hadoop.hdds.scm.ScmConfigKeys;
 import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
 import org.apache.hadoop.hdds.scm.client.ContainerApi;
 import org.apache.hadoop.hdds.scm.client.ScmClient;
 import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManager;
 import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManagerImpl;
 import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
 import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
 import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
 import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
 import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
 import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
 import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
 import org.apache.hadoop.io.IOUtils;
 import org.apache.hadoop.ozone.container.ContainerTestHelper;
 import org.junit.jupiter.api.AfterAll;
 import org.junit.jupiter.api.BeforeAll;
 import org.junit.jupiter.api.Test;
 import org.junit.jupiter.api.Timeout;

 import java.io.IOException;
 import java.util.List;

 import static java.nio.charset.StandardCharsets.UTF_8;
 import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.REPLICATION;
 import static org.assertj.core.api.Assertions.assertThat;
 import static org.junit.jupiter.api.Assertions.assertEquals;
 import static org.junit.jupiter.api.Assertions.assertInstanceOf;
 import static org.junit.jupiter.api.Assertions.assertThrows;
 import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class tests container operations (TODO currently only supports create)
 * from cblock clients.
 */
@Timeout(value = 300)
public class TestContainerOperations {
  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static ContainerApiManager containerApiManager;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(3).build();
    storageClient = new ContainerOperationClient(ozoneConf);
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    containerApiManager = new ContainerApiManagerImpl();
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  void testContainerStateMachineIdempotency() throws Exception {
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
    long containerID = container.getContainerInfo().getContainerID();
    Pipeline pipeline = container.getPipeline();
    ContainerApi containerClient = containerApiManager.acquireClient(pipeline);
    //create the container
    containerClient.createContainer(containerID);
    // call create Container again
    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    byte[] data = RandomStringUtils.secure().next(RandomUtils.secure().randomInt(0, 1024)).getBytes(UTF_8);
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper
            .getWriteChunkRequest(container.getPipeline(), blockID,
                data.length);
    containerClient.writeChunk();

    //Make the write chunk request again without requesting for overWrite
    containerClient.writeChunk();
    // Now, explicitly make a putKey request for the block.
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper
            .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
    containerClient.putBlock();
    // send the putBlock again
    containerClient.putBlock();

    // close container call
    containerClient.closeContainer(containerID);
    containerClient.closeContainer(containerID);
  }

  /**
   * A simple test to create a container with {@link ContainerOperationClient}.
   * @throws Exception
   */
  @Test
  public void testCreate() throws Exception {
    ContainerWithPipeline container = storageClient.createContainer(HddsProtos
        .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
        .ONE, OzoneConsts.OZONE);
    assertEquals(container.getContainerInfo().getContainerID(), storageClient
        .getContainer(container.getContainerInfo().getContainerID())
        .getContainerID());
  }

  /**
   * A simple test to get Pipeline with {@link ContainerOperationClient}.
   * @throws Exception
   */
  @Test
  public void testGetPipeline() throws Exception {
    Exception e =
        assertThrows(Exception.class, () -> storageClient.getPipeline(PipelineID.randomId().getProtobuf()));
    assertInstanceOf(PipelineNotFoundException.class, SCMHAUtils.unwrapException(e));
    assertThat(storageClient.listPipelines()).isNotEmpty();
  }

  @Test
  public void testDatanodeUsageInfoCompatibility() throws IOException {
    DatanodeDetails dn = cluster.getStorageContainerManager()
        .getScmNodeManager()
        .getAllNodes()
        .get(0);
    dn.setCurrentVersion(0);

    List<HddsProtos.DatanodeUsageInfoProto> usageInfoList =
        storageClient.getDatanodeUsageInfo(
            dn.getIpAddress(), dn.getUuidString());

    for (HddsProtos.DatanodeUsageInfoProto info : usageInfoList) {
      assertTrue(info.getNode().getPortsList().stream()
          .anyMatch(port -> REPLICATION.name().equals(port.getName())));
    }

    usageInfoList =
        storageClient.getDatanodeUsageInfo(true, 3);

    for (HddsProtos.DatanodeUsageInfoProto info : usageInfoList) {
      assertTrue(info.getNode().getPortsList().stream()
          .anyMatch(port -> REPLICATION.name().equals(port.getName())));
    }
  }

  @Test
  public void testDatanodeUsageInfoContainerCount() throws IOException {
    List<DatanodeDetails> dnList = cluster.getStorageContainerManager()
            .getScmNodeManager()
            .getAllNodes();

    for (DatanodeDetails dn : dnList) {
      List<HddsProtos.DatanodeUsageInfoProto> usageInfoList =
              storageClient.getDatanodeUsageInfo(
                      dn.getIpAddress(), dn.getUuidString());

      assertEquals(1, usageInfoList.size());
      assertEquals(0, usageInfoList.get(0).getContainerCount());
    }

    storageClient.createContainer(HddsProtos
            .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
            .ONE, OzoneConsts.OZONE);

    int[] totalContainerCount = new int[2];
    for (DatanodeDetails dn : dnList) {
      List<HddsProtos.DatanodeUsageInfoProto> usageInfoList =
              storageClient.getDatanodeUsageInfo(
                      dn.getIpAddress(), dn.getUuidString());

      assertEquals(1, usageInfoList.size());
      assertThat(usageInfoList.get(0).getContainerCount()).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(1);
      totalContainerCount[(int)usageInfoList.get(0).getContainerCount()]++;
    }
    assertEquals(2, totalContainerCount[0]);
    assertEquals(1, totalContainerCount[1]);
  }
}
