package de.mherrmann.tomatofilebackup.persistence;

import de.mherrmann.tomatofilebackup.TestUtil;
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

import static org.junit.jupiter.api.Assertions.*;

public class DatabaseEngineSnapshotTestRemove {

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
    void shouldRemoveSnapshotByHashId() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotByHashId(testObjects.firstSnapshotEntity.getHashId());

        assertRemoved(
                testObjects.firstSnapshotEntity,
                testObjects.firstFileEntityInFirstSnapshot, testObjects.secondFileEntityInFirstSnapshot,
                testObjects.chunkInFirstFileInFirstSnapshot, testObjects.chunkInSecondFileInFirstSnapshot);
        assertRemained(
                testObjects.secondSnapshotEntity,
                testObjects.firstFileEntityInSecondSnapshot, testObjects.secondFileEntityInSecondSnapshot,
                testObjects.chunkInFirstFileInSecondSnapshot, testObjects.chunkInSecondFileInSecondSnapshot);
    }

    @Test
    void removeSnapshotsButKeepLastRecent() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsButKeepLastRecent(1);

        assertRemoved(
                testObjects.firstSnapshotEntity,
                testObjects.firstFileEntityInFirstSnapshot, testObjects.secondFileEntityInFirstSnapshot,
                testObjects.chunkInFirstFileInFirstSnapshot, testObjects.chunkInSecondFileInFirstSnapshot);
        assertRemained(
                testObjects.secondSnapshotEntity,
                testObjects.firstFileEntityInSecondSnapshot, testObjects.secondFileEntityInSecondSnapshot,
                testObjects.chunkInFirstFileInSecondSnapshot, testObjects.chunkInSecondFileInSecondSnapshot);
    }

    @Test
    void shouldRemoveSnapshotsOlderThan() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsOlderThan(testObjects.firstSnapshotEntity.getCtime()+1);

        assertRemoved(
                testObjects.firstSnapshotEntity,
                testObjects.firstFileEntityInFirstSnapshot, testObjects.secondFileEntityInFirstSnapshot,
                testObjects.chunkInFirstFileInFirstSnapshot, testObjects.chunkInSecondFileInFirstSnapshot);
        assertRemained(
                testObjects.secondSnapshotEntity,
                testObjects.firstFileEntityInSecondSnapshot, testObjects.secondFileEntityInSecondSnapshot,
                testObjects.chunkInFirstFileInSecondSnapshot, testObjects.chunkInSecondFileInSecondSnapshot);
    }

    @Test
    void shouldRemoveSnapshotsByUuidsSingle() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsByUuids(testObjects.firstSnapshotEntity.getUuid());

        assertRemoved(
                testObjects.firstSnapshotEntity,
                testObjects.firstFileEntityInFirstSnapshot, testObjects.secondFileEntityInFirstSnapshot,
                testObjects.chunkInFirstFileInFirstSnapshot, testObjects.chunkInSecondFileInFirstSnapshot);
        assertRemained(
                testObjects.secondSnapshotEntity,
                testObjects.firstFileEntityInSecondSnapshot, testObjects.secondFileEntityInSecondSnapshot,
                testObjects.chunkInFirstFileInSecondSnapshot, testObjects.chunkInSecondFileInSecondSnapshot);
    }

    @Test
    void shouldRemoveSnapshotsByUuidsBoth() throws Exception {
        TestObjects testObjects = prepareRemoveTest();

        engine.removeSnapshotsByUuids(testObjects.firstSnapshotEntity.getUuid(), testObjects.secondSnapshotEntity.getUuid());

        assertRemoved(
                testObjects.firstSnapshotEntity,
                testObjects.firstFileEntityInFirstSnapshot, testObjects.secondFileEntityInFirstSnapshot,
                testObjects.chunkInFirstFileInFirstSnapshot, testObjects.chunkInSecondFileInFirstSnapshot);
        assertRemoved(
                testObjects.secondSnapshotEntity,
                testObjects.firstFileEntityInSecondSnapshot, testObjects.secondFileEntityInSecondSnapshot,
                testObjects.chunkInFirstFileInSecondSnapshot, testObjects.chunkInSecondFileInSecondSnapshot);
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
        TestObjects testObjects = new TestObjects();
        createSnapshotEntities(testObjects);
        createFileEntities(testObjects);
        createChunkEntities(testObjects, createChunks());
        return testObjects;
    }

    private void createSnapshotEntities(TestObjects testObjects) throws SQLException {
        testObjects.firstSnapshotEntity = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME);
        testObjects.secondSnapshotEntity = engine.addSnapshot(TEST_SOURCE_PATH, TEST_HOST, TEST_CTIME+1);
    }

    private void createFileEntities(TestObjects testObjects) throws SQLException {
        testObjects.firstFileEntityInFirstSnapshot = engine.addRegularFile(
                DatabaseEngineFileTest.TEST_FILE_PATH,
                DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                false,
                "user",
                "owner",
                DatabaseEngineFileTest.TEST_MOD,
                testObjects.firstSnapshotEntity
        );
        testObjects.secondFileEntityInFirstSnapshot = engine.addRegularFile(
                DatabaseEngineFileTest.TEST_FILE_PATH+2,
                DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE+2,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                false,
                "user",
                "owner",
                DatabaseEngineFileTest.TEST_MOD,
                testObjects.firstSnapshotEntity
        );
        testObjects.firstFileEntityInSecondSnapshot = engine.addRegularFile(
                DatabaseEngineFileTest.TEST_FILE_PATH+3,
                DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE+3,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                false,
                "user",
                "owner",
                DatabaseEngineFileTest.TEST_MOD,
                testObjects.secondSnapshotEntity
        );
        testObjects.secondFileEntityInSecondSnapshot = engine.addRegularFile(
                DatabaseEngineFileTest.TEST_FILE_PATH+4, DatabaseEngineFileTest.TEST_SIZE,
                DatabaseEngineFileTest.TEST_FILE_INODE+4,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                DatabaseEngineFileTest.TEST_MTIME,
                false,
                "user",
                "owner",
                DatabaseEngineFileTest.TEST_MOD,
                testObjects.secondSnapshotEntity
        );
    }

    private Chunk[] createChunks(){
        Chunk chunk1 = new Chunk(0, 10);
        Chunk chunk2 = new Chunk(10, (int)DatabaseEngineChunkTest.TEST_OFFSET-10);
        Chunk chunk3 = new Chunk(DatabaseEngineChunkTest.TEST_OFFSET, DatabaseEngineChunkTest.TEST_LENGTH);
        Chunk chunk4 = new Chunk(DatabaseEngineChunkTest.TEST_OFFSET+DatabaseEngineChunkTest.TEST_LENGTH, DatabaseEngineChunkTest.TEST_LENGTH);
        chunk1.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+2);
        chunk2.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+1);
        chunk3.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+4);
        chunk4.setChecksum(DatabaseEngineChunkTest.TEST_CHECKSUM+3);
        return new Chunk[]{chunk1, chunk2, chunk3, chunk4};
    }

    private void createChunkEntities(TestObjects testObjects, Chunk[] chunks) throws SQLException {
        testObjects.chunkInFirstFileInFirstSnapshot = engine.addChunk(chunks[0], testObjects.firstFileEntityInFirstSnapshot.getUuid());
        testObjects.chunkInSecondFileInFirstSnapshot = engine.addChunk(chunks[1], testObjects.secondFileEntityInFirstSnapshot.getUuid());
        testObjects.chunkInFirstFileInSecondSnapshot = engine.addChunk(chunks[2], testObjects.firstFileEntityInSecondSnapshot.getUuid());
        testObjects.chunkInSecondFileInSecondSnapshot = engine.addChunk(chunks[3], testObjects.secondFileEntityInSecondSnapshot.getUuid());
    }

    private static class TestObjects{
        SnapshotEntity firstSnapshotEntity;
        SnapshotEntity secondSnapshotEntity;

        FileEntity firstFileEntityInFirstSnapshot;
        FileEntity secondFileEntityInFirstSnapshot;
        FileEntity firstFileEntityInSecondSnapshot;
        FileEntity secondFileEntityInSecondSnapshot;

        ChunkEntity chunkInFirstFileInFirstSnapshot;
        ChunkEntity chunkInSecondFileInFirstSnapshot;
        ChunkEntity chunkInFirstFileInSecondSnapshot;
        ChunkEntity chunkInSecondFileInSecondSnapshot;
    }
}
