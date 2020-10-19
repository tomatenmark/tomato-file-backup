package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.entities.ChunkEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.RepositoryEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;

public class DatabaseEngine {

    Connection connection;
    private final ChunkDatabaseEngine chunkDatabaseEngine;
    private final FileDatabaseEngine fileDatabaseEngine;
    private final SnapshotDatabaseEngine snapshotDatabaseEngine;
    private final RepositoryDatabaseEngine repositoryDatabaseEngine;

    public DatabaseEngine(String repositoryPath) throws SQLException {
        File dbFile = new File(repositoryPath, Constants.DB_FILENAME);
        String url = "jdbc:sqlite:"+dbFile.getAbsolutePath();
        this.connection = DriverManager.getConnection(url);
        this.chunkDatabaseEngine = new ChunkDatabaseEngine(connection);
        this.fileDatabaseEngine = new FileDatabaseEngine(connection);
        this.snapshotDatabaseEngine = new SnapshotDatabaseEngine(connection);
        this.repositoryDatabaseEngine = new RepositoryDatabaseEngine(repositoryPath, connection);
        turnOnConstraints();
    }

    public void destroy() throws SQLException {
        connection.close();
    }

    public void initializeRepository() throws IOException, SQLException {
        repositoryDatabaseEngine.initializeRepository();
    }

    public RepositoryEntity getRepository() throws SQLException {
        return repositoryDatabaseEngine.getRepository();
    }

    public ChunkEntity addChunk(Chunk chunk, String fileUuid) throws SQLException {
        return chunkDatabaseEngine.addChunk(chunk, fileUuid);
    }

    public FileEntity addRegularFile(String path, long size, long inode, long mtime,
                               boolean compressed, SnapshotEntity snapshotEntity) throws SQLException {
        return fileDatabaseEngine.addRegularFile(path, size, inode, mtime, compressed, snapshotEntity);
    }

    public FileEntity addDirectory(String path, long inode, long mtime, SnapshotEntity snapshotEntity) throws SQLException {
        return fileDatabaseEngine.addDirectory(path, inode, mtime, snapshotEntity);
    }

    public FileEntity addSymlink(String path, long inode, long mtime, boolean targetIsDirectory,
                                 String linkPath, SnapshotEntity snapshotEntity) throws SQLException {
        return fileDatabaseEngine.addSymlink(path, inode, mtime, targetIsDirectory, linkPath, snapshotEntity);
    }

    public FileEntity addJunction(String path, long inode, long mtime,
                                 String linkPath, SnapshotEntity snapshotEntity) throws SQLException {
        return fileDatabaseEngine.addJunction(path, inode, mtime, linkPath, snapshotEntity);
    }

    public SnapshotEntity addSnapshot(String sourcePath, String host, long ctime) throws SQLException {
        return snapshotDatabaseEngine.addSnapshot(sourcePath, host, ctime);
    }

    public void addChunkFileRelation(String fileUuid, String chunkUuid, long offset) throws SQLException {
        chunkDatabaseEngine.addChunkFileRelation(fileUuid, chunkUuid, offset);
    }

    public void addFileSnapshotRelation(String fileUuid, String snapshotUuid) throws SQLException {
        fileDatabaseEngine.addFileSnapshotRelation(fileUuid, snapshotUuid);
    }

    public ChunkEntity getChunkByChecksum(String checksum) throws SQLException {
        return chunkDatabaseEngine.getChunkByChecksum(checksum);
    }

    public List<ChunkEntity> getChunksByFileUuid(String fileUuid) throws SQLException {
        return chunkDatabaseEngine.getChunksByFileUuid(fileUuid);
    }

    public boolean existsChunkByChecksum(String checksum) throws SQLException {
        return chunkDatabaseEngine.existsChunkByChecksum(checksum);
    }

    public FileEntity getFileByInode(Long inode, SnapshotEntity snapshotEntity) throws SQLException {
        return fileDatabaseEngine.getFileByInode(inode, snapshotEntity);
    }

    public FileEntity getFileByNameAndSizeAndMtime(String name, Long size,
                                                   Long mtime, SnapshotEntity snapshotEntity) throws SQLException {
        return fileDatabaseEngine.getFileByNameAndSizeAndMtime(name, size, mtime, snapshotEntity);
    }

    public List<FileEntity> getFilesBySnapshotUuidOrderByInode(String snapshotUuid) throws SQLException {
        return fileDatabaseEngine.getFilesBySnapshotUuidOrderByInode(snapshotUuid);
    }

    public List<FileEntity> getFilesBySnapshotUuidOrderByPath(String snapshotUuid) throws SQLException {
        return fileDatabaseEngine.getFilesBySnapshotUuidOrderByPath(snapshotUuid);
    }

    public SnapshotEntity getSnapshotByHashId(String hashId) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotByHashId(hashId);
    }

    public List<SnapshotEntity> getAllSnapshots() throws SQLException {
        return snapshotDatabaseEngine.getAllSnapshots();
    }

    public List<SnapshotEntity> getAllSnapshotsSince(long ctimeThreshold) throws SQLException {
        return snapshotDatabaseEngine.getAllSnapshotsSince(ctimeThreshold);
    }

    public List<SnapshotEntity> getSnapshotsBySource(String source) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotsBySource(source);
    }

    public List<SnapshotEntity> getSnapshotsBySourceSince(String source, long ctimeThreshold) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotsBySourceSince(source, ctimeThreshold);
    }

    public List<SnapshotEntity> getSnapshotsByHost(String host) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotsByHost(host);
    }

    public List<SnapshotEntity> getSnapshotsByHostSince(String host, long ctimeThreshold) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotsByHostSince(host, ctimeThreshold);
    }

    public List<SnapshotEntity> getSnapshotsBySourceAndHost(String source, String host) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotsBySourceAndHost(source, host);
    }

    public List<SnapshotEntity> getSnapshotsBySourceAndHostSince(String source, String host, long ctimeThreshold) throws SQLException {
        return snapshotDatabaseEngine.getSnapshotsBySourceAndHostSince(source, host, ctimeThreshold);
    }

    public List<String> removeSnapshotByHashId(String hashId) throws SQLException {
        return snapshotDatabaseEngine.removeSnapshotByHashId(hashId, fileDatabaseEngine, chunkDatabaseEngine);
    }

    public List<String> removeSnapshotsOlderThan(long threshold) throws SQLException {
        return snapshotDatabaseEngine.removeSnapshotsOlderThan(threshold, fileDatabaseEngine, chunkDatabaseEngine);
    }

    private void turnOnConstraints() throws SQLException {
        String sql = "PRAGMA foreign_keys = ON";
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }
}
