/*
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

package org.apache.hadoop.ozone.shell.fsck.writer;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

public class JacksonOzoneFsckWriter implements OzoneFsckWriter {
  @Override
  public void writeKeyInfo(OmKeyInfo keyInfo) {

  }

  @Override
  public void writeCorruptedKey(OmKeyInfo keyInfo) {

  }

  @Override
  public void writeDamagedBlocks(Set<HddsProtos.BlockID> damagedBlocks) {

  }

  @Override
  public void writeLocationInfo(OmKeyLocationInfo locationInfo) {

  }

  @Override
  public void writeContainerInfo(ContainerProtos.ContainerDataProto container, DatanodeDetails datanodeDetails) throws IOException {

  }

  @Override
  public void writeBlockInfo(ContainerProtos.BlockData locationInfo) {

  }

  @Override
  public void writeChunkInfo(ContainerProtos.ChunkInfo chunk) {

  }

  @Override
  public void close() {

  }
}
