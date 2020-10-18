package de.mherrmann.tomatofilebackup.filetransfer;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.chunking.ChecksumEngine;
import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.chunking.ChunkingEngine;
import de.mherrmann.tomatofilebackup.persistence.DatabaseEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

class TransferEngineTest {

    private File sourceFile;
    private final File chunksDirectory = new File("./test/chunks");

    @BeforeEach
    void setUp() throws Exception {
        TestUtil.createTestDirectory();
        Files.createDirectory(new File(chunksDirectory.getAbsolutePath()).toPath());
        sourceFile = TestUtil.buildRandomTestFile(5*1024*1024);
    }

    @AfterEach
    void tearDown(){
        TestUtil.removeTestFiles();
    }

    @Test
    void shouldStoreChunkFromListToTestDirectoryUncompressed() throws Exception {
        Chunk chunk = prepareChunk();
        TransferEngine engine = new TransferEngine();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunk);

        engine.storeChunks(sourceFile, chunksDirectory, chunks, false);

        assertValidStored(chunk, false);
    }

    @Test
    void shouldStoreChunkFromListToTestDirectoryCompressed() throws Exception {
        Chunk chunk = prepareChunk();
        TransferEngine engine = new TransferEngine();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunk);

        engine.storeChunks(sourceFile, chunksDirectory, chunks, true);

        assertValidStored(chunk, true);
    }

    @Test
    void shouldRestoreFileFromUncompressedChunks() throws Exception {
        TransferEngine transferEngine = new TransferEngine();
        ChunkingEngine chunkingEngine = new ChunkingEngine();
        List<Chunk> chunks = chunkingEngine.getChunks(sourceFile);
        transferEngine.storeChunks(sourceFile, chunksDirectory, chunks, false);
        File testFile = new File(sourceFile.getAbsolutePath()+".restored");

        transferEngine.restoreFile(testFile, chunksDirectory, chunks, false);

        assertArrayEquals(Files.readAllBytes(sourceFile.toPath()), Files.readAllBytes(testFile.toPath()));
    }

    @Test
    void shouldRestoreFileFromCompressedChunks() throws Exception {
        TransferEngine transferEngine = new TransferEngine();
        ChunkingEngine chunkingEngine = new ChunkingEngine();
        List<Chunk> chunks = chunkingEngine.getChunks(sourceFile);
        transferEngine.storeChunks(sourceFile, chunksDirectory, chunks, true);
        File testFile = new File(sourceFile.getAbsolutePath()+".restored");

        transferEngine.restoreFile(testFile, chunksDirectory, chunks, true);

        assertArrayEquals(Files.readAllBytes(sourceFile.toPath()), Files.readAllBytes(testFile.toPath()));
    }

    @Test
    void shouldRemoveChunksByChecksums() throws Exception {
        Chunk chunkToBeRemained = prepareChunk();
        Chunk chunkToBeRemoved = prepareChunk();
        chunkToBeRemoved.setChecksum("toRemove");
        TransferEngine engine = new TransferEngine();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunkToBeRemained);
        chunks.add(chunkToBeRemoved);
        engine.storeChunks(sourceFile, chunksDirectory, chunks, true);
        List<String> checksums = new ArrayList<>();
        checksums.add(chunkToBeRemoved.getChecksum());

        engine.removeChunkFiles(chunksDirectory, checksums);

        assertTrue(new File(chunksDirectory, chunkToBeRemained.getChecksum()).exists());
        assertFalse(new File(chunksDirectory, chunkToBeRemoved.getChecksum()).exists());
    }

    @Test
    void shouldRemoveOrphanedChunks() throws Exception {
        Chunk chunkToBeRemained = prepareChunk();
        Chunk chunkToBeRemoved = prepareChunk();
        chunkToBeRemoved.setChecksum("toRemove");
        TransferEngine engine = new TransferEngine();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunkToBeRemained);
        chunks.add(chunkToBeRemoved);
        engine.storeChunks(sourceFile, chunksDirectory, chunks, true);
        DatabaseEngine databaseEngine = new DatabaseEngine(chunksDirectory.getParent());
        databaseEngine.initializeRepository();
        databaseEngine.addChunk(chunkToBeRemained, "fileUuid");

        engine.removeOrphanedChunkFiles(chunksDirectory, databaseEngine);

        assertTrue(new File(chunksDirectory, chunkToBeRemained.getChecksum()).exists());
        assertFalse(new File(chunksDirectory, chunkToBeRemoved.getChecksum()).exists());
    }

    private void assertValidStored(Chunk chunk, boolean compressed) throws IOException {
        File file = new File(chunksDirectory.getAbsolutePath()+"/"+chunk.getChecksum());
        assertTrue(file.exists());
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        byte[] bytes = new byte[(int) randomAccessFile.length()];
        randomAccessFile.read(bytes);
        if(compressed){
            assertNotEquals(bytes.length, chunk.getLength());
            bytes = CompressionEngine.restoreDecompressed(file, chunk.getLength());
        }
        assertEquals(chunk.getLength(), bytes.length);
        String checksum = ChecksumEngine.getChunkChecksum(bytes, 0, bytes.length);
        assertEquals(chunk.getChecksum(), checksum);
    }

    private Chunk prepareChunk() throws IOException {
        Chunk chunk = new Chunk(368123, 468346);
        byte[] bytes = getChunkBytes(chunk);
        chunk.setChecksum(ChecksumEngine.getChunkChecksum(bytes, 0, bytes.length));
        return chunk;
    }

    private byte[] getChunkBytes(Chunk chunk) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(sourceFile, "r");
        randomAccessFile.seek(chunk.getOffset());
        byte[] bytes = new byte[chunk.getLength()];
        randomAccessFile.read(bytes);
        return bytes;
    }

}
