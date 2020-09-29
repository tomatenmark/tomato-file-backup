package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ChecksumEngineTest {

    @Test
    public void shouldGetChecksumFromFixedMessage(){
        byte[] bytes = "HASH".getBytes();

        String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);

        assertEquals("4341BCFB28F64BED93ACA9A378AE7D8C", checksum);
    }

    @Test
    public void shouldGetChecksumFromReproducibleRandomBytes(){
        byte[] bytes = buildReproducibleRandomTestBytes();

        String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);

        assertEquals("BD09C7FCA1866E2CA6FB601FB9E9626F", checksum);
    }

    @Test
    public void shouldSetChecksum(){
        byte[] bytes = buildReproducibleRandomTestBytes();
        Chunk chunk = new Chunk(0, bytes.length);
        AtomicLong processedChecksums = new AtomicLong(0);
        AtomicLong chunkCount = new AtomicLong(1);
        AtomicBoolean finished = new AtomicBoolean(false);
        ChecksumEngine checksumEngine = new ChecksumEngine(processedChecksums, chunkCount, finished);

        checksumEngine.setChecksum(bytes, chunk);
        waitForChecksums(finished);

        assertEquals("BD09C7FCA1866E2CA6FB601FB9E9626F", chunk.getChecksum());
    }

    private byte[] buildReproducibleRandomTestBytes() {
        Random random = new Random(42);
        byte[] bytes = new byte[769557];
        random.nextBytes(bytes);
        return bytes;
    }

    private void waitForChecksums(AtomicBoolean finished) {
        try {
            synchronized(finished) {
                while(!finished.get()){
                    finished.wait();
                }
            }
        } catch(InterruptedException ignored){}
    }

}
