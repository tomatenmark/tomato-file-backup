package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.entities.ChunkEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DatabaseEngine {

    Connection connection;
    private final String repositoryPath;

    public DatabaseEngine(String repositoryPath) throws SQLException {
        File dbFile = new File(repositoryPath, Constants.DB_FILENAME);
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

    public ChunkEntity getChunkByChecksum(String checksum) throws SQLException {
        String sql = "SELECT * FROM chunk WHERE checksum = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, checksum);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        Chunk chunk = new Chunk(resultSet.getLong("offset"), resultSet.getInt("length"));
        chunk.setChecksum(checksum);
        return new ChunkEntity(resultSet.getString("chunk_uuid"), chunk);
    }

    public List<ChunkEntity> getChunksByFileUuid(String fileUuid) throws SQLException {
        List<ChunkEntity> chunks = new ArrayList<>();
        String sql = "SELECT chunk.* From chunk " +
                "LEFT JOIN file_chunk_relation USING(chunk_uuid) " +
                "LEFT JOIN file USING(file_uuid) " +
                "WHERE file_uuid = ? " +
                "ORDER BY ordinal";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, fileUuid);
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            Chunk chunk = new Chunk(resultSet.getLong("offset"), resultSet.getInt("length"));
            chunk.setChecksum(resultSet.getString("checksum"));
            chunks.add(new ChunkEntity(resultSet.getString("chunk_uuid"), chunk));
        }
        return chunks;
    }

    public boolean existsChunkByChecksum(String checksum) throws SQLException {
        String sql = "SELECT * FROM chunk WHERE checksum = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, checksum);
        ResultSet resultSet = preparedStatement.executeQuery();
        return !resultSet.isClosed();
    }

    public List<FileEntity> getFilesBySnapshotUuid(String snapshotUuid) throws SQLException {
        List<FileEntity> files = new ArrayList<>();
        String sql = "SELECT file.* From file " +
                "LEFT JOIN file_snapshot_relation USING(file_uuid) " +
                "LEFT JOIN snapshot USING(snapshot_uuid) " +
                "WHERE snapshot_uuid = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotUuid);
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            FileEntity fileEntity = new FileEntity(
                    resultSet.getString("file_uuid"),
                    resultSet.getString("path"),
                    resultSet.getLong("size"),
                    resultSet.getLong("inode"),
                    resultSet.getLong("mtime"),
                    resultSet.getBoolean("link"),
                    resultSet.getString("link_path"),
                    resultSet.getBoolean("compressed")
            );
            files.add(fileEntity);
        }
        return files;
    }

    public SnapshotEntity getSnapshotByHashId(String hashId) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE hash_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, hashId);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        return buildSnapshotEntity(resultSet);
    }

    public List<SnapshotEntity> getAllSnapshots() throws SQLException {
        String sql = "SELECT * FROM snapshot";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getAllSnapshotsSince(long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE ctime >= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getSnapshotsBySource(String source) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getSnapshotsBySourceSince(String source, long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND ctime >= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        preparedStatement.setLong(2, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getSnapshotsByHost(String host) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE host = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, host);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getSnapshotsByHostSince(String host, long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE host = ? AND ctime >= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, host);
        preparedStatement.setLong(2, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getSnapshotsBySourceAndHost(String source, String host) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND host = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        preparedStatement.setString(2, host);
        return buildSnapshotEntityList(preparedStatement);
    }

    public List<SnapshotEntity> getSnapshotsBySourceAndHostSince(String source, String host, long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND host = ? AND ctime >= ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        preparedStatement.setString(2, host);
        preparedStatement.setLong(2, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    private List<SnapshotEntity> buildSnapshotEntityList(PreparedStatement preparedStatement) throws SQLException {
        List<SnapshotEntity> snapshots = new ArrayList<>();
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            snapshots.add(buildSnapshotEntity(resultSet));
        }
        return snapshots;
    }

    private SnapshotEntity buildSnapshotEntity(ResultSet resultSet) throws SQLException {
        return new SnapshotEntity(
                resultSet.getString("snapshot_uuid"),
                resultSet.getString("source"),
                resultSet.getString("host"),
                resultSet.getLong("ctime")
        );
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
