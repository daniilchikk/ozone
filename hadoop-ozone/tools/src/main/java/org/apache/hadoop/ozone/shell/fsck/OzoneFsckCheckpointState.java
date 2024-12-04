package org.apache.hadoop.ozone.shell.fsck;

public class OzoneFsckCheckpointState {
    private String lastCheckedVolume;
    private String lastCheckedBucket;
    private String lastCheckedKey;

    public void CheckpointState() {
    }

    public void CheckpointState(String lastCheckedVolume, String lastCheckedBucket, String lastCheckedKey) {
        this.lastCheckedVolume = lastCheckedVolume;
        this.lastCheckedBucket = lastCheckedBucket;
        this.lastCheckedKey = lastCheckedKey;
    }

    public String getLastCheckedVolume() {
        return lastCheckedVolume;
    }

    public void setLastCheckedVolume(String lastCheckedVolume) {
        this.lastCheckedVolume = lastCheckedVolume;
    }

    public String getLastCheckedBucket() {
        return lastCheckedBucket;
    }

    public void setLastCheckedBucket(String lastCheckedBucket) {
        this.lastCheckedBucket = lastCheckedBucket;
    }

    public String getLastCheckedKey() {
        return lastCheckedKey;
    }

    public void setLastCheckedKey(String lastCheckedKey) {
        this.lastCheckedKey = lastCheckedKey;
    }
}