/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.scm.client.ContainerApi;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManager;
import org.apache.hadoop.hdds.scm.client.manager.ContainerApiManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data validator of chunks to use pure datanode XCeiver interface.
 */
@Command(name = "dcv",
    aliases = "datanode-chunk-validator",
    description = "Validate generated Chunks are the same ",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class DatanodeChunkValidator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeChunkValidator.class);

  @Option(names = {"-l", "--pipeline"},
          description = "Pipeline to use. By default the first RATIS/THREE pipeline will be used.",
          defaultValue = "")
  private String pipelineId;

  @Option(names = {"-s", "--size"},
          description = "Size of the generated chunks (in bytes)",
          defaultValue = "1024")
  private int chunkSize;

  private ContainerApi containerClient;

  private Timer timer;

  private ChecksumData checksumReference;

  private Checksum checksum;


  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConf = createOzoneConfiguration();
    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
              "Datanode chunk validator is not supported in secure environment"
      );
    }

    try (StorageContainerLocationProtocol scmClient = createStorageContainerLocationClient(ozoneConf)) {
      Pipeline pipeline = findPipelineForTest(pipelineId, scmClient, LOG);

      ContainerApiManager containerApiManager = new ContainerApiManagerImpl();

      this.containerClient = containerApiManager.acquireClient(pipeline);

      readReference();

      timer = getMetrics().timer("chunk-validate");

      runTests(this::validateChunk);

      return null;
    }
  }

  /**
   * Read a reference chunk using same name than one from the
   * {@link org.apache.hadoop.ozone.freon.DatanodeChunkGenerator}.
   */
  private void readReference() throws IOException {
    ReadChunkResponseProto response = containerClient.readChunk(null, null);

    checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, chunkSize);
    checksumReference = computeChecksum(response);
  }

  private void validateChunk(long stepNo) {
    timer.time(() -> {
      try {
        ReadChunkResponseProto response = containerClient.readChunk(null, null);

        ChecksumData checksumOfChunk = computeChecksum(response);

        if (!checksumReference.equals(checksumOfChunk)) {
          throw new IllegalStateException("Reference (=first) message checksum doesn't match with checksum of chunk "
              + response.getChunkData().getChunkName());
        }
      } catch (IOException e) {
        LOG.warn("Could not read chunk due to IOException: ", e);
      }
    });

  }

  private ChecksumData computeChecksum(ReadChunkResponseProto response)
      throws OzoneChecksumException {
    if (response.hasData()) {
      return checksum.computeChecksum(response.getData().asReadOnlyByteBuffer());
    } else {
      return checksum.computeChecksum(response.getDataBuffers().getBuffersList());
    }
  }
}
