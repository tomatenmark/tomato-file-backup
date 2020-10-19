package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class FileDatabaseEngine {

    private final Connection connection;

    FileDatabaseEngine(Connection connection) {
        this.connection = connection;
    }

    FileEntity addRegularFile(String path, long size, long inode, long mtime,
                               boolean compressed, SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, size, inode, mtime, compressed, false, false, false, "", snapshotEntity);
    }

    FileEntity addDirectory(String path, long inode, long mtime, SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, 0, inode, mtime, false, false, false, true, "", snapshotEntity);
    }

    FileEntity addSymlink(String path, long inode, long mtime, boolean targetIsDirectory,
                                 String linkPath, SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, 0, inode, mtime, false, true, false, targetIsDirectory, linkPath, snapshotEntity);
    }

    FileEntity addJunction(String path, long inode, long mtime,
                                 String linkPath, SnapshotEntity snapshotEntity) throws SQLException {
        return addFile(path, 0, inode, mtime, false, false, true, true, linkPath, snapshotEntity);
    }

    void addFileSnapshotRelation(String fileUuid, String snapshotUuid) throws SQLException {
        String relationUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file_snapshot_relation(relation_uuid,file_uuid,snapshot_uuid) VALUES(?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, relationUuid);
        preparedStatement.setString(2, fileUuid);
        preparedStatement.setString(3, snapshotUuid);
        preparedStatement.executeUpdate();
    }

    FileEntity getFileByInode(Long inode, SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT file.* FROM file " +
                "LEFT JOIN file_snapshot_relation USING (file_uuid) " +
                "LEFT JOIN snapshot USING (snapshot_uuid)" +
                "WHERE file.inode = ? AND snapshot.source = ? AND snapshot.host = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, inode);
        preparedStatement.setString(2, snapshotEntity.getSource());
        preparedStatement.setString(3, snapshotEntity.getHost());
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        return buildFileEntity(resultSet);
    }

    FileEntity getFileByNameAndSizeAndMtime(String name, Long size,
                                                   Long mtime, SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT file.* FROM file " +
                "LEFT JOIN file_snapshot_relation USING (file_uuid) " +
                "LEFT JOIN snapshot USING (snapshot_uuid)" +
                "WHERE file.path LIKE ? AND file.size = ? AND file.mtime = ? AND snapshot.source = ? AND snapshot.host = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "%/"+name);
        preparedStatement.setLong(2, size);
        preparedStatement.setLong(3, mtime);
        preparedStatement.setString(4, snapshotEntity.getSource());
        preparedStatement.setString(5, snapshotEntity.getHost());
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        return buildFileEntity(resultSet);
    }

    List<FileEntity> getFilesBySnapshotUuidOrderByInode(String snapshotUuid) throws SQLException {
        String sql = "SELECT file.* From file " +
                "LEFT JOIN file_snapshot_relation USING(file_uuid) " +
                "LEFT JOIN snapshot USING(snapshot_uuid) " +
                "WHERE snapshot_uuid = ? ORDER BY inode";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotUuid);
        return getFilesBySnapshotUuid(preparedStatement);
    }

    List<FileEntity> getFilesBySnapshotUuidOrderByPath(String snapshotUuid) throws SQLException {
        String sql = "SELECT file.* From file " +
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
                resultSet.getLong("mtime"),
                resultSet.getBoolean("link"),
                resultSet.getBoolean("junction"),
                resultSet.getBoolean("directory"),
                resultSet.getString("link_path"),
                resultSet.getBoolean("compressed")
        );
    }

    private FileEntity addFile(String path, long size, long inode, long mtime, boolean compressed, boolean isLink, boolean isJunction,
                               boolean isDirectory, String linkPath, SnapshotEntity snapshotEntity) throws SQLException {
        String fileUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file(file_uuid,path,size,inode,mtime,compressed,link,junction,directory,link_path) VALUES(?,?,?,?,?,?,?,?,?,?);";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, fileUuid);
            preparedStatement.setString(2, path);
            preparedStatement.setLong(3, size);
            preparedStatement.setLong(4, inode);
            preparedStatement.setLong(5, mtime);
            preparedStatement.setBoolean(6, compressed);
            preparedStatement.setBoolean(7, isLink);
            preparedStatement.setBoolean(8, isJunction);
            preparedStatement.setBoolean(9, isDirectory);
            preparedStatement.setString(10, linkPath);
            preparedStatement.executeUpdate();
            addFileSnapshotRelation(fileUuid, snapshotEntity.getUuid());
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            connection.setAutoCommit(true);
            throw new SQLException("Error: Could not add file. path: " + path, exception);
        }
        connection.setAutoCommit(true);
        return getFileByInode(inode, snapshotEntity);
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
