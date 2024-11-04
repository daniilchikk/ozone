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
import java.io.Writer;
import java.util.Set;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

public class PlainTextOzoneFsckWriter implements OzoneFsckWriter {
  private final Writer writer;

  public PlainTextOzoneFsckWriter(Writer writer) {
    this.writer = writer;
  }

  @Override
  public void writeKeyInfo(OmKeyInfo keyInfo) throws IOException {
    keySeparator();
    printLine("Key Information:");
    printLine("  Name: %s/%s/%s", keyInfo.getVolumeName(), keyInfo.getBucketName(), keyInfo.getKeyName());
    printLine("  Path: %s", keyInfo.getPath());
    printLine("  Size: %s", formatSize(keyInfo.getDataSize()));
    printLine("  Type: %s", keyInfo.isFile() ? "File" : "Directory");
    writer.flush();
  }

  @Override
  public void writeCorruptedKey(OmKeyInfo keyInfo) throws IOException {
    writeKeyInfo(keyInfo);
    printLine("Key is corrupted! No blocks present for the key.");
    writer.flush();
  }

  @Override
  public void writeDamagedBlocks(Set<BlockID> damagedBlocks) throws IOException {
    if (!damagedBlocks.isEmpty()) {
      sectionSeparator();
      printLine("Key is damaged!");
      printLine("Damaged blocks:");
      for (BlockID blockID : damagedBlocks) {
        printLine("  %s", blockID.getContainerBlockID());
      }
    }
  }

  @Override
  public void writeLocationInfo(OmKeyLocationInfo locationInfo) throws IOException {
    subsectionSeparator();
    printLine("Location information:");
    printLine("  Pipeline: %s", locationInfo.getPipeline());
    subsectionSeparator();
  }

  @Override
  public void writeContainerInfo(ContainerDataProto container, DatanodeDetails datanodeDetails) throws IOException {
    printLine("  Container %s information:", container.getContainerID());
    printLine("    Path: %s", container.getContainerPath());
    printLine("    Type: %s", container.getContainerType());
    printLine("    State: %s", container.getState());
    printLine("    Datanode: %s (%s)", datanodeDetails.getHostName(), datanodeDetails.getUuidString());
  }

  @Override
  public void writeBlockInfo(BlockData blockInfo) throws IOException {
    subsectionSeparator();
    DatanodeBlockID blockID = blockInfo.getBlockID();

    printLine("  Block %s information:", blockID.getLocalID());
    printLine("    Block commit sequence id: %s", blockID.getBlockCommitSequenceId());
  }

  @Override
  public void writeChunkInfo(ChunkInfo chunk) throws IOException {
    printLine("      Chunk: %s", chunk.getChunkName());
  }

  private void eol() throws IOException {
    writer.write(System.lineSeparator());
    writer.flush();
  }

  private void printLine(String line, Object... args) throws IOException {
    writer.write(String.format(line, args));
    eol();
  }

  private void keySeparator() throws IOException {
    separator("============");
  }

  private void sectionSeparator() throws IOException {
    separator("------------");
  }

  private void subsectionSeparator() throws IOException {
    eol();
  }

  private void separator(String pattern) throws IOException {
    writer.write(pattern);
    eol();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static String formatSize(long v) {
    if (v < 1024) return v + " B";
    int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
    return String.format("%.1f %sB", (double)v / (1L << (z*10)), " KMGTPE".charAt(z));
  }
}
