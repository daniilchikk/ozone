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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

public interface OzoneFsckWriter extends AutoCloseable {
  void writeKeyInfo(OmKeyInfo keyInfo) throws IOException;

  void writeCorruptedKey(OmKeyInfo keyInfo) throws IOException;

  void writeDamagedBlocks(Set<BlockID> damagedBlocks) throws IOException;

  void writeLocationInfo(OmKeyLocationInfo locationInfo) throws IOException;

  void writeContainerInfo(ContainerDataProto container, DatanodeDetails datanodeDetails) throws IOException;

  void writeBlockInfo(BlockData blockInfo) throws IOException;

  void writeChunkInfo(ChunkInfo chunk) throws IOException;

  @Override
  void close() throws IOException;
}
