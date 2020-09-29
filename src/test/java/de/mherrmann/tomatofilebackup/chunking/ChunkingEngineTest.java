package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;

public class ChunkingEngineTest {

    @AfterEach
    public void tearDown(){
        new File("./test.bin").delete();
    }

    @Test
    public void shouldGetChunksForSmallSizeFile() throws Exception {
        RandomAccessFile file = buildRandomTestFile(ChunkingEngine.MIN_CHUNK_SIZE);
        ChunkingEngine engine = new ChunkingEngine();

        ArrayList<Chunk> chunks = engine.getChunks(file);

        assertEquals(1, chunks.size());
        assertEquals(0, chunks.get(0).getOffset());
        assertEquals(ChunkingEngine.MIN_CHUNK_SIZE, chunks.get(0).getLength());
        assertValidChecksums(chunks, file);
    }

    @Test
    public void shouldGetChunksForMediumSizeFile() throws Exception {
        RandomAccessFile file = buildRandomTestFile(ChunkingEngine.PORTION);
        ChunkingEngine engine = new ChunkingEngine();

        ArrayList<Chunk> chunks = engine.getChunks(file);

        assertValidChunking(chunks, file);
    }

    @Test
    public void shouldGetChunksForLargeSizeFile() throws Exception {
        RandomAccessFile file = buildRandomTestFile(ChunkingEngine.PORTION * 2 + ChunkingEngine.MAX_CHUNK_SIZE*10);
        ChunkingEngine engine = new ChunkingEngine();

        ArrayList<Chunk> chunks = engine.getChunks(file);

        assertValidChunking(chunks, file);
    }

    private void assertValidChunking(ArrayList<Chunk> chunks, RandomAccessFile file) throws Exception {
        assertEquals(file.length(), getTotalLength(chunks));
        assertTrue(getMaxChunkSize(chunks) <= ChunkingEngine.MAX_CHUNK_SIZE);
        assertTrue(getAvgChunkSize(chunks, file) >= ChunkingEngine.AVG_CHUNK_SIZE - ChunkingEngine.AVG_CHUNK_SIZE/5);
        assertTrue(getAvgChunkSize(chunks, file) <= ChunkingEngine.AVG_CHUNK_SIZE + ChunkingEngine.AVG_CHUNK_SIZE/5);
        assertContinuous(chunks);
        assertValidChecksums(chunks, file);
    }

    private void assertContinuous(ArrayList<Chunk> chunks){
        long previousOffset = 0;
        long previousLength = 0;
        for(Chunk chunk : chunks){
            assertEquals(chunk.getOffset(), (previousOffset + previousLength));
            previousOffset = chunk.getOffset();
            previousLength = chunk.getLength();
        }
    }

    private void assertValidChecksums(ArrayList<Chunk> chunks, RandomAccessFile file) throws IOException {
        for(Chunk chunk : chunks){
            file.seek(chunk.getOffset());
            byte[] bytes = new byte[chunk.getLength()];
            file.read(bytes);
            String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);
            assertEquals(checksum, chunk.getChecksum());
        }
    }

    private long getMaxChunkSize(ArrayList<Chunk> chunks){
        int max = 0;
        for(Chunk chunk : chunks){
            if(chunk.getLength() > max){
                max = chunk.getLength();
            }
        }
        return max;
    }

    private long getAvgChunkSize(ArrayList<Chunk> chunks, RandomAccessFile file) throws IOException {
        return file.length() / chunks.size();
    }

    private long getTotalLength(ArrayList<Chunk> chunks){
        long totalLength = 0;
        for(Chunk chunk : chunks){
            totalLength += chunk.getLength();
        }
        return totalLength;
    }

    private RandomAccessFile buildRandomTestFile(long length) throws Exception {
        Random random = new Random();
        long remaining = length;
        long maxStepLength = 1024*1024*1024;
        File file = new File("./test.bin");
        file.createNewFile();
        FileOutputStream outputStream = new FileOutputStream(file);
        while(remaining > 0){
            int nextLength = (int) Long.min(remaining, maxStepLength);
            remaining -= nextLength;
            byte[] bytes = new byte[nextLength];
            random.nextBytes(bytes);
            outputStream.write(bytes);
        }
        outputStream.close();
        outputStream.flush();
        return new RandomAccessFile(file, "r");
    }
}
