package de.mherrmann.tomatofilebackup.persistence.entities;

public class FileEntity {
    private String path;
    private long size;
    private long inode;
    private long mtime;
    private boolean link;
    private String linkPath;
    private boolean compressed;

    public FileEntity(String path, long size, long inode, long mtime, boolean link, String linkPath, boolean compressed) {
        this.path = path;
        this.size = size;
        this.inode = inode;
        this.mtime = mtime;
        this.link = link;
        this.linkPath = linkPath;
        this.compressed = compressed;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getInode() {
        return inode;
    }

    public void setInode(long inode) {
        this.inode = inode;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public boolean isLink() {
        return link;
    }

    public void setLink(boolean link) {
        this.link = link;
    }

    public String getLinkPath() {
        return linkPath;
    }

    public void setLinkPath(String linkPath) {
        this.linkPath = linkPath;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }
}
