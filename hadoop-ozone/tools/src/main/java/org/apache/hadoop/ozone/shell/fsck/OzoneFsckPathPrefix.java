package org.apache.hadoop.ozone.shell.fsck;

import jakarta.annotation.Nullable;

public final class OzoneFsckPathPrefix {
  private final @Nullable String volume;

  private final @Nullable String bucket;

  private final @Nullable String key;

  OzoneFsckPathPrefix(@Nullable String volume, @Nullable String bucket, @Nullable String key) {
    this.volume = volume;
    this.bucket = bucket;
    this.key = key;
  }

  @Nullable String volume() {
    return volume;
  }

  @Nullable String bucket() {
    return bucket;
  }

  @Nullable String key() {
    return key;
  }
}
