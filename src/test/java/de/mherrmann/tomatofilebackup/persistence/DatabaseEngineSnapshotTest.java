package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseEngineSnapshotTest {

    private static final String TEST_REPOSITORY_PATH = "./test/";
    private static final String TEST_SOURCE_PATH = "/home/max/";
    private static final String TEST_HOST = "pcmax";
    private static final long TEST_CTIME = 1987654321;

    private DatabaseEngine engine;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        TestUtil.createTestDirectory();
        engine = new DatabaseEngine(TEST_REPOSITORY_PATH);
        engine.initializeRepository();
    }

    @AfterEach
    void tearDown() throws SQLException {
        TestUtil.removeTestFiles();
        engine.destroy();
    }

    @Test
    void shouldAddSnapshot() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        assertValidSnapshot();
    }

    @Test
    void shouldGetSnapshotByHashId() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        SnapshotEntity snapshot = engine.getSnapshotByHashId(snapshotEntity.getHashId());

        assertEquals(TEST_CTIME, snapshot.getCtime());
    }

    @Test
    void shouldGetAllSnapshots() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getAllSnapshots();

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetAllSnapshotsSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getAllSnapshotsSince(TEST_CTIME);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetSnapshotsBySource() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySource(TEST_SOURCE_PATH);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetSnapshotsBySourceSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceSince(TEST_SOURCE_PATH, TEST_CTIME);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetSnapshotsByHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHost(TEST_HOST);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetSnapshotsByHostSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHostSince(TEST_HOST, TEST_CTIME);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetSnapshotsBySourceAndHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHost(TEST_SOURCE_PATH, TEST_HOST);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldGetSnapshotsBySourceAndHostSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHostSince(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        assertEquals(2, snapshots.size());
        assertEquals(TEST_CTIME+1, snapshots.get(0).getCtime());
        assertEquals(TEST_CTIME, snapshots.get(1).getCtime());
    }

    @Test
    void shouldFailGetSnapshotByHashId() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        SnapshotEntity snapshot = null;
        try {
            snapshot = engine.getSnapshotByHashId("invalid");
        } catch(SQLException ignored){}


        assertNull(snapshot);
    }

    @Test
    void shouldGetEmptySnapshotsResultSetCausedByEmptyDb() throws SQLException {
        List<SnapshotEntity> snapshots = engine.getAllSnapshots();

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchSource() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySource("none");

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHost("none");

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchHostRegardlessOfSource() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHost(TEST_SOURCE_PATH, "none");

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchHostRegardlessOfRecentEnough() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHostSince("none", TEST_CTIME);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchHostRegardlessOfAndSourceRecentEnough() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHostSince(TEST_SOURCE_PATH, "none", TEST_CTIME);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchSourceRegardlessOfHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHost("none", TEST_HOST);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchSourceRegardlessOfRecentEnough() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceSince("none", TEST_CTIME);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptyResultSetCausedByNoSuchSourceRegardlessOfHostAndRecentEnough() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHostSince("none", TEST_HOST, TEST_CTIME);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptySnapshotsResultSetCausedByToOldSnapshots() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getAllSnapshotsSince(TEST_CTIME+1);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptySnapshotsResultSetCausedByToOldSnapshotsRegardlessOfSource() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceSince(TEST_SOURCE_PATH, TEST_CTIME+1);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptySnapshotsResultSetCausedByToOldSnapshotsRegardlessOfHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHostSince(TEST_HOST, TEST_CTIME+1);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldGetEmptySnapshotsResultSetCausedByToOldSnapshotsRegardlessOfSourceAndHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHostSince(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        assertTrue(snapshots.isEmpty());
    }

    @Test
    void shouldRemoveSnapshotByHashId() throws SQLException {
        SnapshotEntity snapshotEntityExpectedToBeRemoved = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        SnapshotEntity snapshotEntityExpectedToBeRemained = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);

        engine.removeSnapshotByHashId(snapshotEntityExpectedToBeRemoved.getHashId());

        assertRemoved(snapshotEntityExpectedToBeRemoved);
        assertRemained(snapshotEntityExpectedToBeRemained);
    }

    private void assertValidSnapshot() throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE source = ? AND host = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, TEST_SOURCE_PATH);
        preparedStatement.setString(2, TEST_HOST);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        String hashId = ChecksumEngine.getSnapshotChecksum(resultSet.getString("snapshot_uuid"));
        assertEquals(TEST_CTIME, resultSet.getLong("ctime"));
        assertEquals(hashId, resultSet.getString("hash_id"));
    }

    private void assertRemoved(SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotEntity.getUuid());
        ResultSet resultSet = preparedStatement.executeQuery();
        assertTrue(resultSet.isClosed());
    }

    private void assertRemained(SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotEntity.getUuid());
        ResultSet resultSet = preparedStatement.executeQuery();
        assertFalse(resultSet.isClosed());
    }
}
