package de.mherrmann.tomatofilebackup.persistence.entities;

public class SnapshotEntity {
    private String uuid;
    private String hashId;
    private String source;
    private String host;
    private long ctime;

    public SnapshotEntity(String uuid, String hashId, String source, String host, long ctime) {
        this.uuid = uuid;
        this.hashId = hashId;
        this.source = source;
        this.host = host;
        this.ctime = ctime;
    }

    public String getUuid() {
        return uuid;
    }

    public String getHashId() {
        return hashId;
    }

    public String getSource() {
        return source;
    }

    public String getHost() {
        return host;
    }

    public long getCtime() {
        return ctime;
    }
}
