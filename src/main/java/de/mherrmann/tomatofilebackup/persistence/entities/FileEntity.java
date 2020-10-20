package de.mherrmann.tomatofilebackup.persistence.entities;

public class FileEntity {
    private final String uuid;
    private final String path;
    private final long size;
    private final long inode;
    private final long ctime;
    private final long mtime;
    private final long atime;
    private final boolean compressed;
    private final boolean link;
    private final String linkPath;
    private final boolean junction;
    private final boolean directory;
    private final String ownerUser;
    private final String ownerGroup;
    private final String mod;

    public FileEntity(String uuid, String path, long size, long inode, long ctime, long mtime, long atime, boolean compressed,
                      boolean link, String linkPath, boolean junction, boolean directory, String ownerUser, String ownerGroup, String mod) {
        this.uuid = uuid;
        this.path = path;
        this.size = size;
        this.inode = inode;
        this.ctime = mtime;
        this.mtime = mtime;
        this.atime = mtime;
        this.compressed = compressed;
        this.link = link;
        this.linkPath = linkPath;
        this.junction = junction;
        this.directory = directory;
        this.ownerUser = ownerUser;
        this.ownerGroup = ownerGroup;
        this.mod = mod;
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

    public long getCtime() {
        return ctime;
    }

    public long getMtime() {
        return mtime;
    }

    public long getAtime() {
        return atime;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public boolean isLink() {
        return link;
    }

    public String getLinkPath() {
        return linkPath;
    }

    public boolean isJunction() {
        return junction;
    }

    public boolean isDirectory() {
        return directory;
    }

    public String getOwnerUser() {
        return ownerUser;
    }

    public String getOwnerGroup() {
        return ownerGroup;
    }

    public String getMod() {
        return mod;
    }
}
