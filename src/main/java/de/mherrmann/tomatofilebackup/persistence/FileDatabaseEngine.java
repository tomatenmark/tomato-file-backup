package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class FileDatabaseEngine {

    private final Connection connection;

    FileDatabaseEngine(Connection connection) {
        this.connection = connection;
    }

    FileEntity addRegularFile(String path, long size, long inode, long ctime, long mtime, long atime,
                               boolean compressed, String ownerUser, String ownerGroup, String mod,
                               SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, size, inode, ctime, mtime, atime, compressed, false, "", false,
                false, ownerUser, ownerGroup, mod, snapshotEntity);
    }

    FileEntity addDirectory(String path, long inode, long ctime, long mtime, long atime,
                            String ownerUser, String ownerGroup, String mod, SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, 0, inode, ctime, mtime, atime, false, false, "", false,
                true, ownerUser, ownerGroup, mod, snapshotEntity);
    }

    FileEntity addSymlink(String path, long inode, long ctime, long mtime, long atime, boolean targetIsDirectory,
                                 String linkPath, String ownerUser, String ownerGroup, String mod,
                                 SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, 0, inode, ctime, mtime, atime, false, true, linkPath, false,
                targetIsDirectory, ownerUser, ownerGroup, mod, snapshotEntity);
    }

    FileEntity addJunction(String path, long inode, long ctime, long mtime, long atime,
                                 String linkPath, String ownerUser, String ownerGroup, String mod,
                                 SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, 0, inode, ctime, mtime, atime, false, true, linkPath, true,
                true, ownerUser, ownerGroup, mod, snapshotEntity);
    }

    void addFileSnapshotRelation(String fileUuid, String snapshotUuid, String path) throws SQLException {
        String relationUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file_snapshot_relation(relation_uuid,file_uuid,snapshot_uuid,path) VALUES(?,?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, relationUuid);
        preparedStatement.setString(2, fileUuid);
        preparedStatement.setString(3, snapshotUuid);
        preparedStatement.setString(4, path);
        preparedStatement.executeUpdate();
    }

    Optional<FileEntity> getFileBySizeAndMtimeAndInode(long size, long mtime, long inode, SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT path,file.* FROM file " +
                "LEFT JOIN file_snapshot_relation USING (file_uuid) " +
                "LEFT JOIN snapshot USING (snapshot_uuid)" +
                "WHERE file.size = ? AND file.mtime = ? AND file.inode = ? AND snapshot.source = ? AND snapshot.host = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, size);
        preparedStatement.setLong(2, mtime);
        preparedStatement.setLong(3, inode);
        preparedStatement.setString(4, snapshotEntity.getSource());
        preparedStatement.setString(5, snapshotEntity.getHost());
        ResultSet resultSet = preparedStatement.executeQuery();
        if(!resultSet.next()) {
            return Optional.empty();
        }
        return Optional.of(buildFileEntity(resultSet));
    }

    Optional<FileEntity> getFileBySizeAndMtimeAndName(long size, long mtime, String name, SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT path,file.* FROM file " +
                "LEFT JOIN file_snapshot_relation USING (file_uuid) " +
                "LEFT JOIN snapshot USING (snapshot_uuid)" +
                "WHERE file.size = ? AND file.mtime = ? AND file.name = ? AND snapshot.source = ? AND snapshot.host = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, size);
        preparedStatement.setLong(2, mtime);
        preparedStatement.setString(3, name);
        preparedStatement.setString(4, snapshotEntity.getSource());
        preparedStatement.setString(5, snapshotEntity.getHost());
        ResultSet resultSet = preparedStatement.executeQuery();
        if(!resultSet.next()) {
            return Optional.empty();
        }
        return Optional.of(buildFileEntity(resultSet));
    }

    List<FileEntity> getFilesBySnapshotUuidOrderByInode(String snapshotUuid) throws SQLException {
        String sql = "SELECT path,file.* From file " +
                "LEFT JOIN file_snapshot_relation USING(file_uuid) " +
                "LEFT JOIN snapshot USING(snapshot_uuid) " +
                "WHERE snapshot_uuid = ? ORDER BY inode";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotUuid);
        return getFilesBySnapshotUuid(preparedStatement);
    }

    List<FileEntity> getFilesBySnapshotUuidOrderByPath(String snapshotUuid) throws SQLException {
        String sql = "SELECT path,file.* From file " +
                "LEFT JOIN file_snapshot_relation USING(file_uuid) " +
                "LEFT JOIN snapshot USING(snapshot_uuid) " +
                "WHERE snapshot_uuid = ? ORDER BY path";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotUuid);
        return getFilesBySnapshotUuid(preparedStatement);
    }

    void removeOrphanedFiles() throws SQLException {
        try {
            String sql = "DELETE FROM file WHERE file_uuid IN (" +
                    "SELECT file.file_uuid FROM file " +
                    "LEFT JOIN file_snapshot_relation USING (file_uuid) " +
                    "WHERE snapshot_uuid IS NULL" +
                    ")";
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
        } catch(SQLException exception){
            throw new SQLException("Could not remove orphaned files.", exception);
        }
    }

    private FileEntity buildFileEntity(ResultSet resultSet) throws SQLException {
        return new FileEntity(
                resultSet.getString("file_uuid"),
                resultSet.getString("path"),
                resultSet.getLong("size"),
                resultSet.getLong("inode"),
                resultSet.getLong("ctime"),
                resultSet.getLong("mtime"),
                resultSet.getLong("atime"),
                resultSet.getBoolean("compressed"),
                resultSet.getBoolean("link"),
                resultSet.getString("link_path"),
                resultSet.getBoolean("junction"),
                resultSet.getBoolean("directory"),
                resultSet.getString("owner_user"),
                resultSet.getString("owner_group"),
                resultSet.getString("mod")
        );
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private FileEntity addFile(String path, long size, long inode, long ctime, long mtime, long atime, boolean compressed,
                               boolean isLink, String linkPath, boolean isJunction, boolean isDirectory,
                               String ownerUser, String ownerGroup, String mod, SnapshotEntity snapshotEntity) throws SQLException {
        String fileUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO " +
                "file(file_uuid,name,size,inode,ctime,mtime,atime,compressed,link,link_path,junction,directory,owner_user,owner_group,mod) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int i = 0;
            preparedStatement.setString(++i, fileUuid);
            preparedStatement.setString(++i, new File(path).getName());
            preparedStatement.setLong(++i, size);
            preparedStatement.setLong(++i, inode);
            preparedStatement.setLong(++i, ctime);
            preparedStatement.setLong(++i, mtime);
            preparedStatement.setLong(++i, atime);
            preparedStatement.setBoolean(++i, compressed);
            preparedStatement.setBoolean(++i, isLink);
            preparedStatement.setString(++i, linkPath);
            preparedStatement.setBoolean(++i, isJunction);
            preparedStatement.setBoolean(++i, isDirectory);
            preparedStatement.setString(++i, ownerUser);
            preparedStatement.setString(++i, ownerGroup);
            preparedStatement.setString(++i, mod);
            preparedStatement.executeUpdate();
            addFileSnapshotRelation(fileUuid, snapshotEntity.getUuid(), path);
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            connection.setAutoCommit(true);
            throw new SQLException("Error: Could not add file", exception);
        }
        connection.setAutoCommit(true);
        return getFileBySizeAndMtimeAndInode(size, mtime, inode, snapshotEntity).get();
    }

    private List<FileEntity> getFilesBySnapshotUuid(PreparedStatement preparedStatement) throws SQLException {
        List<FileEntity> files = new ArrayList<>();
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            FileEntity fileEntity = buildFileEntity(resultSet);
            files.add(fileEntity);
        }
        return files;
    }
}
