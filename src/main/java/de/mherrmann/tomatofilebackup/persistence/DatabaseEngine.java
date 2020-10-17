package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.Properties;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class DatabaseEngine {

    Connection connection;
    private final String repositoryPath;

    public DatabaseEngine(String repositoryPath) throws SQLException {
        File dbFile = new File(repositoryPath, Properties.DB_FILENAME);
        String url = "jdbc:sqlite:"+dbFile.getAbsolutePath();
        this.connection = DriverManager.getConnection(url);
        this.repositoryPath = repositoryPath;
    }

    public void destroy() throws SQLException {
        connection.close();
    }

    public void initializeRepository() throws IOException, SQLException {
        RepositoryInitializer.initialize(repositoryPath);
    }

    public void addChunk(Chunk chunk, String fileUuid, int ordinal) throws SQLException {
        String chunkUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO chunk(chunk_uuid,checksum,offset,length) VALUES(?,?,?,?);";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, chunkUuid);
            preparedStatement.setString(2, chunk.getChecksum());
            preparedStatement.setLong(3, chunk.getOffset());
            preparedStatement.setInt(4, chunk.getLength());
            preparedStatement.executeUpdate();
            addChunkFileRelation(fileUuid, chunkUuid, ordinal);
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            throw new SQLException("Error: Could not add chunk. checksum: " + chunk.getChecksum(), exception);
        }
        connection.setAutoCommit(true);
    }

    public void addRegularFile(String path, long size, long inode, long mtime,
                               boolean compressed, String snapshotUuid) throws SQLException {
        addFile(path, size, inode, mtime, compressed, false, "", snapshotUuid);
    }

    public void addSymlink(String path, long size, long inode, long mtime,
                               boolean compressed, String linkPath, String snapshotUuid) throws SQLException {
        addFile(path, size, inode, mtime, compressed, true, linkPath, snapshotUuid);
    }

    public void addSnapshot(String sourcePath, String host, long ctime) throws SQLException {
        String snapshotUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO snapshot(snapshot_uuid,hash_id,source,host,ctime) VALUES(?,?,?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotUuid);
        preparedStatement.setString(2, ChecksumEngine.getSnapshotChecksum(snapshotUuid));
        preparedStatement.setString(3, sourcePath);
        preparedStatement.setString(4, host);
        preparedStatement.setLong(5, ctime);
        preparedStatement.executeUpdate();
    }

    public void addChunkFileRelation(String fileUuid, String chunkUuid, int ordinal) throws SQLException {
        String relationUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file_chunk_relation(relation_uuid,file_uuid,chunk_uuid,ordinal) VALUES(?,?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, relationUuid);
        preparedStatement.setString(2, fileUuid);
        preparedStatement.setString(3, chunkUuid);
        preparedStatement.setInt(4, ordinal);
        preparedStatement.executeUpdate();
    }

    public void addFileSnapshotRelation(String fileUuid, String snapshotUuid) throws SQLException {
        String relationUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file_snapshot_relation(relation_uuid,file_uuid,snapshot_uuid) VALUES(?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, relationUuid);
        preparedStatement.setString(2, fileUuid);
        preparedStatement.setString(3, snapshotUuid);
        preparedStatement.executeUpdate();
    }

    private void addFile(String path, long size, long inode, long mtime,
                         boolean compressed, boolean isLink, String linkPath, String snapshotUuid) throws SQLException {
        String fileUuid = UUID.randomUUID().toString();
        String sql = "INSERT INTO file(file_uuid,path,size,inode,mtime,compressed,link,link_path) VALUES(?,?,?,?,?,?,?,?);";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, fileUuid);
            preparedStatement.setString(2, path);
            preparedStatement.setLong(3, size);
            preparedStatement.setLong(4, inode);
            preparedStatement.setLong(5, mtime);
            preparedStatement.setInt(6, compressed ? 1 : 0);
            preparedStatement.setInt(7, isLink ? 1 : 0);
            preparedStatement.setString(8, linkPath);
            preparedStatement.executeUpdate();
            addFileSnapshotRelation(fileUuid, snapshotUuid);
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            throw new SQLException("Error: Could not add file. path: " + path, exception);
        }
        connection.setAutoCommit(true);
    }
}
