package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

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

        List<SnapshotEntity> snapshots = engine.getAllSnapshots();

        assertEquals(1, snapshots.size());
        assertEquals(TEST_CTIME, snapshots.get(0).getCtime());
    }

    @Test
    void shouldGetAllSnapshotsSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getAllSnapshotsSince(TEST_CTIME);

        assertEquals(1, snapshots.size());
        assertEquals(TEST_CTIME, snapshots.get(0).getCtime());
    }

    @Test
    void shouldGetSnapshotsBySource() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySource(TEST_SOURCE_PATH);

        assertEquals(1, snapshots.size());
        assertEquals(TEST_CTIME, snapshots.get(0).getCtime());
    }

    @Test
    void shouldGetSnapshotsBySourceSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceSince(TEST_SOURCE_PATH, TEST_CTIME);

        assertEquals(1, snapshots.size());
        assertEquals(TEST_CTIME, snapshots.get(0).getCtime());
    }

    @Test
    void shouldGetSnapshotsByHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHost(TEST_HOST);

        assertEquals(1, snapshots.size());
    }

    @Test
    void shouldGetSnapshotsByHostSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsByHostSince(TEST_HOST, TEST_CTIME);

        assertEquals(1, snapshots.size());
    }

    @Test
    void shouldGetSnapshotsBySourceAndHost() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHost(TEST_SOURCE_PATH, TEST_HOST);

        assertEquals(1, snapshots.size());
    }

    @Test
    void shouldGetSnapshotsBySourceAndHostSince() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHostSince(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        assertEquals(1, snapshots.size());
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
}
