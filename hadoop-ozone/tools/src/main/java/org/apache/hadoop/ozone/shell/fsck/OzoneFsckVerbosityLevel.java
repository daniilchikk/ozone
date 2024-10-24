package org.apache.hadoop.ozone.shell.fsck;

public enum OzoneFsckVerbosityLevel {
  KEY(0),
  CONTAINER(1),
  BLOCK(2),
  CHUNK(3);

  private final int level;

  OzoneFsckVerbosityLevel(int level) {
    this.level = level;
  }

  boolean applicable(OzoneFsckVerbosityLevel expectedLevel) {
    return level >= expectedLevel.level;
  }
}
