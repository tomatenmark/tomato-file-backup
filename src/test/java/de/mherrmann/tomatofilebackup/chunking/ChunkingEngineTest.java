package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

class ChunkingEngineTest {

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    public void tearDown(){
        new File("./test.bin").delete();
    }

    @Test
    void shouldGetChunksForSmallSizeFile() throws Exception {
        RandomAccessFile file = TestUtil.buildRandomTestRandomAccessFile(ChunkingEngine.MIN_CHUNK_SIZE);
        ChunkingEngine engine = new ChunkingEngine();

        List<Chunk> chunks = engine.getChunks(file);

        assertEquals(1, chunks.size());
        assertEquals(0, chunks.get(0).getOffset());
        assertEquals(ChunkingEngine.MIN_CHUNK_SIZE, chunks.get(0).getLength());
        assertValidChecksums(chunks, file);
    }

    @Test
    void shouldGetChunksForMediumSizeFile() throws Exception {
        RandomAccessFile file = TestUtil.buildRandomTestRandomAccessFile(ChunkingEngine.PORTION);
        ChunkingEngine engine = new ChunkingEngine();

        List<Chunk> chunks = engine.getChunks(file);

        assertValidChunking(chunks, file);
    }

    @Test
    void shouldGetChunksForLargeSizeFile() throws Exception {
        RandomAccessFile file = TestUtil.buildRandomTestRandomAccessFile(ChunkingEngine.PORTION * 2 + ChunkingEngine.MAX_CHUNK_SIZE*10);
        ChunkingEngine engine = new ChunkingEngine();

        List<Chunk> chunks = engine.getChunks(file);

        assertValidChunking(chunks, file);
    }

    private void assertValidChunking(List<Chunk> chunks, RandomAccessFile file) throws Exception {
        assertEquals(file.length(), getTotalLength(chunks));
        assertTrue(getMaxChunkSize(chunks) <= ChunkingEngine.MAX_CHUNK_SIZE);
        assertTrue(getAvgChunkSize(chunks, file) >= ChunkingEngine.AVG_CHUNK_SIZE - ChunkingEngine.AVG_CHUNK_SIZE/5);
        assertTrue(getAvgChunkSize(chunks, file) <= ChunkingEngine.AVG_CHUNK_SIZE + ChunkingEngine.AVG_CHUNK_SIZE/5);
        assertContinuous(chunks);
        assertValidChecksums(chunks, file);
    }

    private void assertContinuous(List<Chunk> chunks){
        long previousOffset = 0;
        long previousLength = 0;
        for(Chunk chunk : chunks){
            assertEquals(chunk.getOffset(), (previousOffset + previousLength));
            previousOffset = chunk.getOffset();
            previousLength = chunk.getLength();
        }
    }

    private void assertValidChecksums(List<Chunk> chunks, RandomAccessFile file) throws IOException {
        for(Chunk chunk : chunks){
            file.seek(chunk.getOffset());
            byte[] bytes = new byte[chunk.getLength()];
            file.read(bytes);
            String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);
            assertEquals(checksum, chunk.getChecksum());
        }
    }

    private long getMaxChunkSize(List<Chunk> chunks){
        int max = 0;
        for(Chunk chunk : chunks){
            if(chunk.getLength() > max){
                max = chunk.getLength();
            }
        }
        return max;
    }

    private long getAvgChunkSize(List<Chunk> chunks, RandomAccessFile file) throws IOException {
        return file.length() / chunks.size();
    }

    private long getTotalLength(List<Chunk> chunks){
        long totalLength = 0;
        for(Chunk chunk : chunks){
            totalLength += chunk.getLength();
        }
        return totalLength;
    }
}
