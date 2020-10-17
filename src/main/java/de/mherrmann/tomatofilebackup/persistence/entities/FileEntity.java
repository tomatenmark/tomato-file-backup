package de.mherrmann.tomatofilebackup.persistence.entities;

public class FileEntity {
    private String uuid;
    private String path;
    private long size;
    private long inode;
    private long mtime;
    private boolean link;
    private String linkPath;
    private boolean compressed;

    public FileEntity(String uuid, String path, long size, long inode, long mtime, boolean link, String linkPath, boolean compressed) {
        this.uuid = uuid;
        this.path = path;
        this.size = size;
        this.inode = inode;
        this.mtime = mtime;
        this.link = link;
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

    public String getLinkPath() {
        return linkPath;
    }

    public boolean isCompressed() {
        return compressed;
    }
}
