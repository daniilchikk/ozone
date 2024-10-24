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

package org.apache.hadoop.ozone.shell.fsck;

import static org.apache.hadoop.ozone.shell.fsck.OzoneFsckVerbosityLevel.BLOCK;
import static org.apache.hadoop.ozone.shell.fsck.OzoneFsckVerbosityLevel.CHUNK;
import static org.apache.hadoop.ozone.shell.fsck.OzoneFsckVerbosityLevel.CONTAINER;

/**
 * It is a configuration holder class used to manage verbose settings for the Ozone File System Check (Fsck) operations.
 * It is used to determine the level of verbosity during the scan operations performed by the {@link OzoneFsckHandler}.
 */
public final class OzoneFsckVerboseSettings {
  private final boolean printHealthyKeys;

  private final OzoneFsckVerbosityLevel level;

  private OzoneFsckVerboseSettings(boolean printHealthyKeys, OzoneFsckVerbosityLevel level) {
    this.printHealthyKeys = printHealthyKeys;
    this.level = level;
  }

  public boolean printHealthyKeys() {
    return printHealthyKeys;
  }

  public boolean printContainers() {
    return level.applicable(CONTAINER);
  }

  public boolean printBlocks() {
    return level.applicable(BLOCK);
  }

  public boolean printChunks() {
    return level.applicable(CHUNK);
  }

  public static SettingsBuilder builder() {
    return new SettingsBuilder();
  }

  public static class SettingsBuilder {
    private boolean printKeys;

    private OzoneFsckVerbosityLevel level;

    public SettingsBuilder printHealthyKeys(boolean printHealthyKeys) {
      this.printKeys = printHealthyKeys;

      return this;
    }

    public SettingsBuilder level(OzoneFsckVerbosityLevel level) {
      this.level = level;

      return this;
    }
    public OzoneFsckVerboseSettings build() {
      return new OzoneFsckVerboseSettings(printKeys, level);
    }
  }
}
