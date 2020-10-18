package de.mherrmann.tomatofilebackup.persistence.entities;

public class FileEntity {
    private final String uuid;
    private final String path;
    private final long size;
    private final long inode;
    private final long mtime;
    private final boolean link;
    private final boolean junction;
    private final boolean directory;
    private final String linkPath;
    private final boolean compressed;

    public FileEntity(String uuid, String path, long size, long inode, long mtime,
                      boolean link, boolean junction, boolean directory, String linkPath, boolean compressed) {
        this.uuid = uuid;
        this.path = path;
        this.size = size;
        this.inode = inode;
        this.mtime = mtime;
        this.link = link;
        this.junction = junction;
        this.directory = directory;
        this.linkPath = linkPath;
        this.compressed = compressed;
    }

    public String getUuid() {
        return uuid;
    }

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    public long getInode() {
        return inode;
    }

    public long getMtime() {
        return mtime;
    }

    public boolean isLink() {
        return link;
    }

    public boolean isJunction() {
        return junction;
    }

    public boolean isDirectory() {
        return directory;
    }

    public String getLinkPath() {
        return linkPath;
    }

    public boolean isCompressed() {
        return compressed;
    }
}
