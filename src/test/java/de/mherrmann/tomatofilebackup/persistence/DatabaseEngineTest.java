package de.mherrmann.tomatofilebackup.persistence;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.chunking.Chunk;
import org.junit.jupiter.api.*;

import de.mherrmann.tomatofilebackup.TestUtil;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DatabaseEngineTest {

    private static final String TEST_REPOSITORY_PATH = "./test/";
    private static final String TEST_CHECKSUM = "testChecksum";
    private static final String TEST_FILE_UUID = UUID.randomUUID().toString();
    private static final long TEST_OFFSET = 456;
    private static final int TEST_LENGTH = 123;
    private static final int TEST_SINGLE_CHUNK_ORDINAL = 42;
    private static final String TEST_FILE_PATH = new File("./test/testFile.txt").getAbsolutePath();
    private static final long TEST_SIZE = 123456;
    private static final long TEST_INODE = 234654;
    private static final long TEST_MTIME = 1234567890;
    private static final String TEST_SNAPSHOT_UUID = UUID.randomUUID().toString();

    private DatabaseEngine engine;

    @BeforeAll
    void setUpAll() throws SQLException, IOException {
        TestUtil.createTestDirectory();
        engine = new DatabaseEngine(TEST_REPOSITORY_PATH);
        engine.initializeRepository();
    }

    @AfterAll
    void tearDownAll() throws SQLException {
        TestUtil.removeTestFiles();
        engine.destroy();
    }

    @Test
    void shouldAddChunk() throws SQLException {
        Chunk chunk = new Chunk(TEST_OFFSET, TEST_LENGTH);
        chunk.setChecksum(TEST_CHECKSUM);

        engine.addChunk(chunk, TEST_FILE_UUID, TEST_SINGLE_CHUNK_ORDINAL);

        assertValidChunk();
    }

    @Test
    void shouldAddChunkFileRelation(){

    }

    @Test
    void shouldAddFile() throws SQLException {
        engine.addFile(TEST_FILE_PATH, TEST_SIZE, TEST_INODE, TEST_MTIME, false, TEST_SNAPSHOT_UUID);

        assertValidFile();
    }

    private void assertValidChunk() throws SQLException {
        String sql = "SELECT * FROM chunk WHERE checksum = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, TEST_CHECKSUM);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(TEST_OFFSET, resultSet.getLong("offset"));
        assertEquals(TEST_LENGTH, resultSet.getInt("length"));
        assertValidChunkFileRelation(resultSet.getString("chunk_uuid"));
    }

    private void assertValidFile() throws SQLException {
        String sql = "SELECT * FROM file WHERE inode = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setLong(1, TEST_INODE);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(TEST_FILE_PATH, resultSet.getString("path"));
        assertEquals(TEST_SIZE, resultSet.getLong("size"));
        assertEquals(TEST_MTIME, resultSet.getLong("mtime"));
        assertEquals(0, resultSet.getInt("compressed"));
        assertValidSnapshotFileRelation(resultSet.getString("file_uuid"));
    }

    private void assertValidChunkFileRelation(String chunkUuid) throws SQLException {
        String sql = "SELECT * FROM file_chunk_relation WHERE file_uuid = ? AND chunk_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, TEST_FILE_UUID);
        preparedStatement.setString(2, chunkUuid);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(TEST_SINGLE_CHUNK_ORDINAL, resultSet.getInt("ordinal"));
    }

    private void assertValidSnapshotFileRelation(String fileUuid) throws SQLException {
        String sql = "SELECT * FROM file_snapshot_relation WHERE file_uuid = ? AND snapshot_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, fileUuid);
        preparedStatement.setString(2, TEST_SNAPSHOT_UUID);
        ResultSet resultSet = preparedStatement.executeQuery();
        assertFalse(resultSet.isClosed());
    }
}
