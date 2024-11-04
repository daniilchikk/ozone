package org.apache.hadoop.ozone.shell.fsck;

/**
 * Presents available options for the output format of ozone fscheck command.
 */
public enum OzoneFsckOutputFormat {
  PLAIN_TEXT,
  JSON,
  XML;

  /** Returns a default option for the output format of ozone fscheck command. */
  public static OzoneFsckOutputFormat getDefault() {
    return JSON;
  }
}
