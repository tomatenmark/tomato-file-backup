package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseEngineSnapshotTestGet {

    private static final String TEST_REPOSITORY_PATH = "./test/";
    private static final String TEST_SOURCE_PATH = "/home/max/";
    private static final String TEST_HOST = "pcmax";
    private static final long TEST_CTIME = 1987654321;

    private DatabaseEngine engine;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        TestUtil.createTestDirectory();
        RepositoryInitializer.initialize(TEST_REPOSITORY_PATH);
        engine = new DatabaseEngine(TEST_REPOSITORY_PATH);
    }

    @AfterEach
    void tearDown() throws SQLException {
        engine.destroy();
        TestUtil.removeTestFiles();
    }

    @Test
    void shouldGetSnapshotByHashId() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        Optional<SnapshotEntity> snapshot = engine.getSnapshotByHashId(snapshotEntity.getHashId());

        assertTrue(snapshot.isPresent());
        assertEquals(TEST_CTIME, snapshot.get().getCtime());
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
    void shouldReturnEmptyOptionalByInvalidHashId() throws SQLException {
        engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);

        Optional<SnapshotEntity> snapshot = engine.getSnapshotByHashId("invalid");

        assertFalse(snapshot.isPresent());
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

        List<SnapshotEntity> snapshots = engine.getSnapshotsBySourceAndHostSince(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME + 1);

        assertTrue(snapshots.isEmpty());
    }
}
