package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SnapshotDatabaseEngine {

    Connection connection;

    public SnapshotDatabaseEngine(Connection connection) throws SQLException {
        this.connection = connection;
    }

    public SnapshotEntity addSnapshot(String sourcePath, String host, long ctime) throws SQLException {
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

    public SnapshotEntity getSnapshotByHashId(String hashId) throws SQLException {
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
        preparedStatement.setLong(3, ctimeThreshold);
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
                resultSet.getString("hash_id"),
                resultSet.getString("source"),
                resultSet.getString("host"),
                resultSet.getLong("ctime")
        );
    }
}