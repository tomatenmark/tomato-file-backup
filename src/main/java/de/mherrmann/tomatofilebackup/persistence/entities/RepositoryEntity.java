package de.mherrmann.tomatofilebackup.persistence.entities;

public class RepositoryEntity {
    private String uuid;
    private String path;
    private String version;

    public RepositoryEntity(String uuid, String path, String version) {
        this.uuid = uuid;
        this.path = path;
        this.version = version;
    }

    public String getUuid() {
        return uuid;
    }

    public String getPath() {
        return path;
    }

    public String getVersion() {
        return version;
    }
}
