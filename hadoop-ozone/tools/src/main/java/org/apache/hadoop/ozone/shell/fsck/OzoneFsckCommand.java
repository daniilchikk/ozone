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

package org.apache.hadoop.ozone.shell.fsck;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.OzoneShell;
import org.apache.hadoop.ozone.shell.Shell;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * The {@link OzoneFsckCommand} class is a command-line tool for performing file system checks within Ozone.
 * This tool supports various options to fine-tune the file system check process,
 * including filters for volumes, buckets, and keys.
 * It can also produce verbose output detailing keys, containers, blocks, and chunks as well as delete corrupted keys.
 * <p>
 * Options:
 * <ul>
 * <li>`--volume-prefix`: Specifies the prefix for volumes that should be included in the check.
 * <li>`--bucket-prefix`: Specifies the prefix for buckets that should be included in the check.
 * <li>`--key-prefix`: Specifies the prefix for keys that should be included in the check.
 * <li>`--delete`: Deletes the corrupted keys.
 * <li>`--keys`: Displays information about good and healthy keys.
 * <li>`--containers`: Includes information about containers.
 * <li>`--blocks`: Includes information about blocks.
 * <li>`--chunks`: Includes information about chunks.
 * <li>`--verbose` or `-v`: Provides full verbose output, ignoring the --keys, --containers, --blocks,
 * and --chunks options.
 * <li>`--output` or `-o`: Specifies the file to output information about the scan process.
 * If not specified, the information will be printed to the system output.
 *</ul>
 */
@CommandLine.Command(name = "fscheck",
    description = "Operational tool to run system-wide file check in Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(SubcommandWithParent.class)
public class OzoneFsckCommand extends Handler implements SubcommandWithParent {

  @CommandLine.Option(names = {"--volume-prefix"},
      description = "Specifies the prefix for volumes that should be included in the check")
  private String volumePrefix;

  @CommandLine.Option(names = {"--bucket-prefix"},
      description = "Specifies the prefix for buckets that should be included in the check")
  private String bucketPrefix;

  @CommandLine.Option(names = {"--key-prefix"},
      description = "Specifies the prefix for keys that should be included in the check")
  private String keyPrefix;

  @CommandLine.Option(names = {"--delete"},
      description = "Deletes the corrupted keys")
  private boolean delete;

  @CommandLine.Option(names = {"--keys"},
      description = "Specifies whether to display information about good and healthy keys")
  private boolean keys;

  @CommandLine.Option(names = {"--containers"},
      description = "Specifies whether to include information about containers")
  private boolean containers;

  @CommandLine.Option(names = {"--blocks"},
      description = "Specifies whether to include information about blocks")
  private boolean blocks;

  @CommandLine.Option(names = {"--chunks"},
      description = "Specifies whether to include information about chunks")
  private boolean chunks;

  @CommandLine.Option(names = {"--verbose", "-v"},
      description = "Full verbose output; ignores --keys, --containers, --blocks, --chunks options")
  private boolean verbose;

  @CommandLine.Option(names = {"--output", "-o"},
      description = "Specifies the file to output information about the scan process."
          + " If not specified, the information will be printed to the system output")
  private String output;

  @CommandLine.ParentCommand
  private Shell shell;

  @Override
  public boolean isVerbose() {
    return shell.isVerbose();
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    return shell.createOzoneConfiguration();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException, OzoneClientException {
    OzoneFsckVerboseSettings verboseSettings = new OzoneFsckVerboseSettings();

    OzoneConfiguration ozoneConfiguration = getConf();

    try (Writer writer = new PrintWriter(System.out);
         OzoneFsckHandler handler =
             new OzoneFsckHandler(address, verboseSettings, writer, delete, client, ozoneConfiguration)) {
      try {
        handler.scan();
      } finally {
        writer.flush();
      }
    } catch (Exception e) {
      throw new IOException("Can't execute fscheck command", e);
    }
  }

  @Override
  public Class<?> getParentType() {
    return OzoneShell.class;
  }
}
