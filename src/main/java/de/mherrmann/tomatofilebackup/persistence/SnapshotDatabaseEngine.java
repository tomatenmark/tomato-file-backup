package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

class SnapshotDatabaseEngine {

    private final Connection connection;

    SnapshotDatabaseEngine(Connection connection) {
        this.connection = connection;
    }

    SnapshotEntity addSnapshot(String sourcePath, String host, long ctime) throws SQLException {
        String snapshotUuid = UUID.randomUUID().toString();
        String hashId = ChecksumEngine.getSnapshotChecksum(snapshotUuid);
        String sql = "INSERT INTO snapshot(snapshot_uuid,hash_id,source,host,ctime) VALUES(?,?,?,?,?);";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotUuid);
        preparedStatement.setString(2, hashId);
        preparedStatement.setString(3, sourcePath);
        preparedStatement.setString(4, host);
        preparedStatement.setLong(5, ctime);
        preparedStatement.executeUpdate();
        return getSnapshotByHashId(hashId);
    }

    SnapshotEntity getSnapshotByHashId(String hashId) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE hash_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, hashId);
        ResultSet resultSet = preparedStatement.executeQuery();
        if(resultSet.isClosed()){
            throw new SQLException("Error: There is no snapshot with id " + hashId);
        }
        resultSet.next();
        return buildSnapshotEntity(resultSet);
    }

    List<SnapshotEntity> getAllSnapshots() throws SQLException {
        String sql = "SELECT * FROM snapshot ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getAllSnapshotsSince(long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE ctime >= ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getSnapshotsBySource(String source) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getSnapshotsBySourceSince(String source, long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND ctime >= ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        preparedStatement.setLong(2, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getSnapshotsByHost(String host) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE host = ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, host);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getSnapshotsByHostSince(String host, long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE host = ? AND ctime >= ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, host);
        preparedStatement.setLong(2, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getSnapshotsBySourceAndHost(String source, String host) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND host = ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        preparedStatement.setString(2, host);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<SnapshotEntity> getSnapshotsBySourceAndHostSince(String source, String host, long ctimeThreshold) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND host = ? AND ctime >= ? ORDER BY ctime DESC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, source);
        preparedStatement.setString(2, host);
        preparedStatement.setLong(3, ctimeThreshold);
        return buildSnapshotEntityList(preparedStatement);
    }

    List<String> removeSnapshotByHashId(String hashId, FileDatabaseEngine fileDatabaseEngine,
                                       ChunkDatabaseEngine chunkDatabaseEngine) throws SQLException {
        connection.setAutoCommit(false);
        String sql = "DELETE FROM snapshot WHERE hash_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, hashId);
        return removeSnapshots(preparedStatement, fileDatabaseEngine, chunkDatabaseEngine);
    }

    List<String> removeSnapshotsOlderThan(long threshold, FileDatabaseEngine fileDatabaseEngine,
                                ChunkDatabaseEngine chunkDatabaseEngine) throws SQLException {
        connection.setAutoCommit(false);
        String sql = "DELETE FROM snapshot WHERE ctime < ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, threshold);
        return removeSnapshots(preparedStatement, fileDatabaseEngine, chunkDatabaseEngine);
    }

    List<String> removeSnapshotsByUuids(String[] uuids, FileDatabaseEngine fileDatabaseEngine,
                                          ChunkDatabaseEngine chunkDatabaseEngine) throws SQLException {
        if(uuids.length == 0){
            return Collections.emptyList();
        }
        connection.setAutoCommit(false);
        String sql = "DELETE FROM snapshot WHERE snapshot_uuid IN (" + getPlaceholders(uuids.length) + ")";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        addUuidsToStatement(uuids, preparedStatement);
        return removeSnapshots(preparedStatement, fileDatabaseEngine, chunkDatabaseEngine);
    }

    //TODO: add removeSnapshotsButKeepNRecent(int i, FileDatabaseEngine fileDatabaseEngine, ChunkDatabaseEngine chunkDatabaseEngine)

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
                resultSet.getString("hash_id"),
                resultSet.getString("source"),
                resultSet.getString("host"),
                resultSet.getLong("ctime")
        );
    }

    private List<String> removeSnapshots(PreparedStatement preparedStatement, FileDatabaseEngine fileDatabaseEngine,
                                 ChunkDatabaseEngine chunkDatabaseEngine) throws SQLException {
        List<String> checksums;
        try {
            preparedStatement.executeUpdate();
            fileDatabaseEngine.removeOrphanedFiles();
            checksums = chunkDatabaseEngine.removeOrphanedChunks();
            connection.commit();
        } catch(SQLException exception){
            connection.rollback();
            connection.setAutoCommit(true);
            throw new SQLException("Could not remove snapshot due to sql error.", exception);
        }
        connection.setAutoCommit(true);
        return checksums;
    }

    private void addUuidsToStatement(String[] uuids, PreparedStatement preparedStatement) throws SQLException {
        int i = 0;
        for(String uuid : uuids){
            preparedStatement.setString(++i, uuid);
        }
    }

    private String getPlaceholders(int n){
        StringBuilder placeholderBuilder = new StringBuilder();
        for(int i = 1; i <= n; i++){
            placeholderBuilder.append("?");
            if(i != n){
                placeholderBuilder.append(",");
            }
        }
        return placeholderBuilder.toString();
    }
}
