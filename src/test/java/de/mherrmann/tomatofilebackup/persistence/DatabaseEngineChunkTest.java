package de.mherrmann.tomatofilebackup.persistence;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.entities.ChunkEntity;
import de.mherrmann.tomatofilebackup.persistence.entities.SnapshotEntity;
import org.junit.jupiter.api.*;

import de.mherrmann.tomatofilebackup.TestUtil;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class DatabaseEngineChunkTest {

    private static final String TEST_REPOSITORY_PATH = "./test/";
    static final String TEST_CHECKSUM = "testChecksum";
    static final String TEST_FILE_UUID = UUID.randomUUID().toString();
    static final long TEST_OFFSET = 456;
    static final int TEST_LENGTH = 123;
    private static final String TEST_FILE_PATH = new File("./test/testFile.txt").getAbsolutePath();
    private static final long TEST_SIZE = 123456;
    private static final long TEST_FILE_INODE = 234654;
    private static final long TEST_MTIME = 1234567890;

    private DatabaseEngine engine;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        TestUtil.createTestDirectory();
        engine = new DatabaseEngine(TEST_REPOSITORY_PATH);
        engine.initializeRepository();
        TestUtil.turnOfConstraints(engine.connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        TestUtil.removeTestFiles();
        engine.destroy();
    }

    @Test
    void shouldAddChunk() throws SQLException {
        Chunk chunk = new Chunk(TEST_OFFSET, TEST_LENGTH);
        chunk.setChecksum(TEST_CHECKSUM);

        engine.addChunk(chunk, TEST_FILE_UUID);

        assertValidChunk();
    }

    @Test
    void shouldAddChunkFileRelation() throws SQLException {
        Chunk chunk = new Chunk(TEST_OFFSET, TEST_LENGTH);
        chunk.setChecksum(TEST_CHECKSUM);
        ChunkEntity chunkEntity = engine.addChunk(chunk, TEST_FILE_UUID);

        engine.addChunkFileRelation(TEST_FILE_UUID, chunkEntity.getUuid(), chunkEntity.getOffset());

        assertValidChunkFileRelation(chunkEntity.getUuid());
    }

    @Test
    void shouldNotBePresentByChecksum() throws SQLException {
        Chunk chunk = new Chunk(TEST_OFFSET, TEST_LENGTH);
        chunk.setChecksum(TEST_CHECKSUM);
        engine.addChunk(chunk, TEST_FILE_UUID);

        Optional<ChunkEntity> returnedChunk = engine.getChunkByChecksum("invalid");

        assertFalse(returnedChunk.isPresent());
    }

    @Test
    void shouldGetChunkByChecksum() throws SQLException {
        Chunk chunk = new Chunk(TEST_OFFSET, TEST_LENGTH);
        chunk.setChecksum(TEST_CHECKSUM);
        engine.addChunk(chunk, TEST_FILE_UUID);

        Optional<ChunkEntity> returnedChunk = engine.getChunkByChecksum(TEST_CHECKSUM);

        assertTrue(returnedChunk.isPresent());
        assertEquals(chunk.getChecksum(), returnedChunk.get().getChecksum());
        assertEquals(chunk.getOffset(), returnedChunk.get().getOffset());
        assertEquals(chunk.getLength(), returnedChunk.get().getLength());
    }

    @Test
    void shouldGetChunksInOrder() throws SQLException {
        SnapshotEntity snapshotEntity = engine.addSnapshot("test", "test", 1234567890);
        engine.addRegularFile(TEST_FILE_PATH, TEST_SIZE, TEST_FILE_INODE, TEST_MTIME, TEST_MTIME,
                TEST_MTIME,false, "user", "owner", "rwxrwxrwx", snapshotEntity);
        Chunk chunk = new Chunk(TEST_OFFSET, TEST_LENGTH);
        Chunk chunk2 = new Chunk(TEST_OFFSET+TEST_LENGTH, TEST_LENGTH);
        chunk.setChecksum(TEST_CHECKSUM);
        chunk2.setChecksum(TEST_CHECKSUM+2);
        engine.addChunk(chunk2, TEST_FILE_UUID);
        engine.addChunk(chunk, TEST_FILE_UUID);

        List<ChunkEntity> chunks = engine.getChunksByFileUuid(TEST_FILE_UUID);

        assertEquals(2, chunks.size());
        assertEquals(TEST_CHECKSUM, chunks.get(0).getChecksum());
        assertEquals(TEST_CHECKSUM+2, chunks.get(1).getChecksum());
    }

    private void assertValidChunk() throws SQLException {
        String sql = "SELECT chunk.*, offset FROM chunk " +
                "LEFT JOIN file_chunk_relation USING (chunk_uuid)" +
                "WHERE checksum = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, TEST_CHECKSUM);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(TEST_OFFSET, resultSet.getLong("offset"));
        assertEquals(TEST_LENGTH, resultSet.getInt("length"));
        assertValidChunkFileRelation(resultSet.getString("chunk_uuid"));
    }

    private void assertValidChunkFileRelation(String chunkUuid) throws SQLException {
        String sql = "SELECT * FROM file_chunk_relation WHERE file_uuid = ? AND chunk_uuid = ?";
        PreparedStatement preparedStatement = engine.connection.prepareStatement(sql);
        preparedStatement.setString(1, TEST_FILE_UUID);
        preparedStatement.setString(2, chunkUuid);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        assertEquals(TEST_OFFSET, resultSet.getInt("offset"));
    }
}
