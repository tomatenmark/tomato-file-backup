package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.entities.ChunkEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.FileEntity;
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
        engine.destroy();
        TestUtil.removeTestFiles();
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
    void shouldRemoveSnapshotByHashId() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotByHashId(testObjects.snapshotEntityExpectedToBeRemoved.getHashId());

        assertRemoved(
                testObjects.snapshotEntityExpectedToBeRemoved,
                testObjects.fileEntityToBeRemoved1, testObjects.fileEntityToBeRemoved2,
                testObjects.chunkEntityToBeRemoved1, testObjects.chunkEntityToBeRemoved2);
        assertRemained(
                testObjects.snapshotEntityExpectedToBeRemained,
                testObjects.fileEntityToBeRemained1, testObjects.fileEntityToBeRemained2,
                testObjects.chunkEntityToBeRemained1, testObjects.chunkEntityToBeRemained2);
    }

    @Test
    void shouldRemoveSnapshotsOlderThan() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsOlderThan(testObjects.snapshotEntityExpectedToBeRemoved.getCtime()+1);

        assertRemoved(
                testObjects.snapshotEntityExpectedToBeRemoved,
                testObjects.fileEntityToBeRemoved1, testObjects.fileEntityToBeRemoved2,
                testObjects.chunkEntityToBeRemoved1, testObjects.chunkEntityToBeRemoved2);
        assertRemained(
                testObjects.snapshotEntityExpectedToBeRemained,
                testObjects.fileEntityToBeRemained1, testObjects.fileEntityToBeRemained2,
                testObjects.chunkEntityToBeRemained1, testObjects.chunkEntityToBeRemained2);
    }

    @Test
    void shouldRemoveSnapshotsByUuidsSingle() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsByUuids(testObjects.snapshotEntityExpectedToBeRemoved.getUuid());

        assertRemoved(
                testObjects.snapshotEntityExpectedToBeRemoved,
                testObjects.fileEntityToBeRemoved1, testObjects.fileEntityToBeRemoved2,
                testObjects.chunkEntityToBeRemoved1, testObjects.chunkEntityToBeRemoved2);
        assertRemained(
                testObjects.snapshotEntityExpectedToBeRemained,
                testObjects.fileEntityToBeRemained1, testObjects.fileEntityToBeRemained2,
                testObjects.chunkEntityToBeRemained1, testObjects.chunkEntityToBeRemained2);
    }

    @Test
    void shouldRemoveSnapshotsByUuidsBoth() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsByUuids(testObjects.snapshotEntityExpectedToBeRemoved.getUuid(), testObjects.snapshotEntityExpectedToBeRemained.getUuid());

        assertRemoved(
                testObjects.snapshotEntityExpectedToBeRemoved,
                testObjects.fileEntityToBeRemoved1, testObjects.fileEntityToBeRemoved2,
                testObjects.chunkEntityToBeRemoved1, testObjects.chunkEntityToBeRemoved2);
        assertRemoved(
                testObjects.snapshotEntityExpectedToBeRemained,
                testObjects.fileEntityToBeRemained1, testObjects.fileEntityToBeRemained2,
                testObjects.chunkEntityToBeRemained1, testObjects.chunkEntityToBeRemained2);
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

    private void assertRemoved(SnapshotEntity snapshotEntity,
                               FileEntity fileEntity1, FileEntity fileEntity2,
                               ChunkEntity chunkEntity1, ChunkEntity chunkEntity2) throws SQLException {
        assertRemovedSnapshot(snapshotEntity);
        assertRemovedFileSnapshotRelations(snapshotEntity);
        assertRemovedFiles(fileEntity1,fileEntity2);
        assertRemovedFileChunkRelations(fileEntity1,fileEntity2);
        assertRemovedChunks(chunkEntity1, chunkEntity2);
    }

    private void assertRemained(SnapshotEntity snapshotEntity,
                                FileEntity fileEntity1, FileEntity fileEntity2,
                                ChunkEntity chunkEntity1, ChunkEntity chunkEntity2) throws SQLException {
        assertRemainedSnapshot(snapshotEntity);
        assertRemainedFileSnapshotRelations(snapshotEntity);
        assertRemainedFiles(fileEntity1,fileEntity2);
        assertRemainedFileChunkRelations(fileEntity1,fileEntity2);
        assertRemainedChunks(chunkEntity1, chunkEntity2);
    }

    private void assertRemovedSnapshot(SnapshotEntity snapshotEntity) throws SQLException {
        ResultSet resultSet = getSnapshotResultSet(snapshotEntity);
        assertTrue(resultSet.isClosed());
    }

    private void assertRemovedFileSnapshotRelations(SnapshotEntity snapshotEntity) throws SQLException {
        ResultSet resultSet = getFileSnapshotRelationResultSet(snapshotEntity);
        assertTrue(resultSet.isClosed());
    }

    private void assertRemovedFiles(FileEntity fileEntity1, FileEntity fileEntity2) throws SQLException {
        ResultSet resultSet = getFileResultSet(fileEntity1, fileEntity2);
        assertTrue(resultSet.isClosed());
    }

    private void assertRemovedFileChunkRelations(FileEntity fileEntity1, FileEntity fileEntity2) throws SQLException {
        ResultSet resultSet = getFileChunkRelationResultSet(fileEntity1, fileEntity2);
        assertTrue(resultSet.isClosed());
    }

    private void assertRemovedChunks(ChunkEntity chunkEntity1, ChunkEntity chunkEntity2) throws SQLException {
        ResultSet resultSet = getChunkResultSet(chunkEntity1, chunkEntity2);
        assertTrue(resultSet.isClosed());
    }

    private void assertRemainedSnapshot(SnapshotEntity snapshotEntity) throws SQLException {
        ResultSet resultSet = getSnapshotResultSet(snapshotEntity);
        assertFalse(resultSet.isClosed());
    }

    private void assertRemainedFileSnapshotRelations(SnapshotEntity snapshotEntity) throws SQLException {
        ResultSet resultSet = getFileSnapshotRelationResultSet(snapshotEntity);
        assertFalse(resultSet.isClosed());
    }

    private void assertRemainedFiles(FileEntity fileEntity1, FileEntity fileEntity2) throws SQLException {
        ResultSet resultSet = getFileResultSet(fileEntity1, fileEntity2);
        assertFalse(resultSet.isClosed());
    }

    private void assertRemainedFileChunkRelations(FileEntity fileEntity1, FileEntity fileEntity2) throws SQLException {
        ResultSet resultSet = getFileChunkRelationResultSet(fileEntity1, fileEntity2);
        assertFalse(resultSet.isClosed());
    }

    private void assertRemainedChunks(ChunkEntity chunkEntity1, ChunkEntity chunkEntity2) throws SQLException {
        ResultSet resultSet = getChunkResultSet(chunkEntity1, chunkEntity2);
        assertFalse(resultSet.isClosed());
    }

    private ResultSet getSnapshotResultSet(SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT * FROM snapshot WHERE snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotEntity.getUuid());
        return preparedStatement.executeQuery();
    }

    private ResultSet getFileSnapshotRelationResultSet(SnapshotEntity snapshotEntity) throws SQLException {
        String sql = "SELECT * FROM file_snapshot_relation WHERE snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, snapshotEntity.getUuid());
        return preparedStatement.executeQuery();
    }

    private ResultSet getFileResultSet(FileEntity fileEntity1, FileEntity fileEntity2) throws SQLException {
        String sql = "SELECT file.* FROM file WHERE file_uuid = ? OR file_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileEntity1.getUuid());
        preparedStatement.setString(2, fileEntity2.getUuid());
        return preparedStatement.executeQuery();
    }

    private ResultSet getFileChunkRelationResultSet(FileEntity fileEntity1, FileEntity fileEntity2) throws SQLException {
        String sql = "SELECT file_chunk_relation.* FROM file_chunk_relation WHERE file_uuid = ? OR file_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileEntity1.getUuid());
        preparedStatement.setString(2, fileEntity2.getUuid());
        return preparedStatement.executeQuery();
    }

    private ResultSet getChunkResultSet(ChunkEntity chunkEntity1, ChunkEntity chunkEntity2) throws SQLException {
        String sql = "SELECT chunk.* FROM chunk WHERE chunk_uuid = ? OR chunk_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, chunkEntity1.getUuid());
        preparedStatement.setString(2, chunkEntity2.getUuid());
        return preparedStatement.executeQuery();
    }

    private TestObjects prepareRemoveTest() throws Exception {
        SnapshotEntity snapshotEntityExpectedToBeRemoved = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        SnapshotEntity snapshotEntityExpectedToBeRemained = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);
        FileEntity fileEntityToBeRemoved1 = engine.addRegularFile(DatabaseEngineFileTest.TEST_FILE_PATH, DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE, DatabaseEngineFileTest.TEST_MTIME, DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME, false, "user", "owner", "rwxrwxrwx", snapshotEntityExpectedToBeRemoved);
        FileEntity fileEntityToBeRemoved2 = engine.addRegularFile(DatabaseEngineFileTest.TEST_FILE_PATH+2, DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE+2, DatabaseEngineFileTest.TEST_MTIME, DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,false, "user", "owner", "rwxrwxrwx", snapshotEntityExpectedToBeRemoved);
        FileEntity fileEntityToBeRemained1 = engine.addRegularFile(DatabaseEngineFileTest.TEST_FILE_PATH+3, DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE+3, DatabaseEngineFileTest.TEST_MTIME, DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,false, "user", "owner", "rwxrwxrwx", snapshotEntityExpectedToBeRemained);
        FileEntity fileEntityToBeRemained2 = engine.addRegularFile(DatabaseEngineFileTest.TEST_FILE_PATH+4, DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE+4, DatabaseEngineFileTest.TEST_MTIME, DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,false, "user", "owner", "rwxrwxrwx", snapshotEntityExpectedToBeRemained);
        Chunk chunk = new Chunk(0, 10);
        Chunk chunk2 = new Chunk(10, (int)DatabaseEngineChunkTest.TEST_OFFSET-10);
        Chunk chunk3 = new Chunk(DatabaseEngineChunkTest.TEST_OFFSET, DatabaseEngineChunkTest.TEST_LENGTH);
        Chunk chunk4 = new Chunk(DatabaseEngineChunkTest.TEST_OFFSET+DatabaseEngineChunkTest.TEST_LENGTH, DatabaseEngineChunkTest.TEST_LENGTH);
        chunk.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM);
        chunk2.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+2);
        chunk3.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+3);
        chunk4.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+4);
        ChunkEntity chunkEntityToBeRemoved1 = engine.addChunk(chunk2, fileEntityToBeRemoved1.getUuid());
        ChunkEntity chunkEntityToBeRemoved2 = engine.addChunk(chunk, fileEntityToBeRemoved2.getUuid());
        ChunkEntity chunkEntityToBeRemained1 = engine.addChunk(chunk4, fileEntityToBeRemained1.getUuid());
        ChunkEntity chunkEntityToBeRemained2 = engine.addChunk(chunk3, fileEntityToBeRemained2.getUuid());
        return new TestObjects(snapshotEntityExpectedToBeRemoved, snapshotEntityExpectedToBeRemained,
                fileEntityToBeRemoved1, fileEntityToBeRemoved2, chunkEntityToBeRemoved1, chunkEntityToBeRemoved2,
                fileEntityToBeRemained1, fileEntityToBeRemained2, chunkEntityToBeRemained1, chunkEntityToBeRemained2);
    }

    private static class TestObjects{
        final SnapshotEntity snapshotEntityExpectedToBeRemoved;
        final SnapshotEntity snapshotEntityExpectedToBeRemained;
        final FileEntity fileEntityToBeRemoved1;
        final FileEntity fileEntityToBeRemoved2;
        final ChunkEntity chunkEntityToBeRemoved1;
        final ChunkEntity chunkEntityToBeRemoved2;

        final FileEntity fileEntityToBeRemained1;
        final FileEntity fileEntityToBeRemained2;
        final ChunkEntity chunkEntityToBeRemained1;
        final ChunkEntity chunkEntityToBeRemained2;

        TestObjects(SnapshotEntity snapshotEntityExpectedToBeRemoved, SnapshotEntity snapshotEntityExpectedToBeRemained,
                    FileEntity fileEntityToBeRemoved1, FileEntity fileEntityToBeRemoved2, ChunkEntity chunkEntityToBeRemoved1, ChunkEntity chunkEntityToBeRemoved2,
                    FileEntity fileEntityToBeRemained1, FileEntity fileEntityToBeRemained2, ChunkEntity chunkEntityToBeRemained1, ChunkEntity chunkEntityToBeRemained2) {
            this.snapshotEntityExpectedToBeRemoved = snapshotEntityExpectedToBeRemoved;
            this.snapshotEntityExpectedToBeRemained = snapshotEntityExpectedToBeRemained;
            this.fileEntityToBeRemoved1 = fileEntityToBeRemoved1;
            this.fileEntityToBeRemoved2 = fileEntityToBeRemoved2;
            this.chunkEntityToBeRemoved1 = chunkEntityToBeRemoved1;
            this.chunkEntityToBeRemoved2 = chunkEntityToBeRemoved2;
            this.fileEntityToBeRemained1 = fileEntityToBeRemained1;
            this.fileEntityToBeRemained2 = fileEntityToBeRemained2;
            this.chunkEntityToBeRemained1 = chunkEntityToBeRemained1;
            this.chunkEntityToBeRemained2 = chunkEntityToBeRemained2;
        }
    }
}
