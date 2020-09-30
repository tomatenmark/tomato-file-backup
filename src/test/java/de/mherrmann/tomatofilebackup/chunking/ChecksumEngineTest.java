package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.Random;

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

    private byte[] buildReproducibleRandomTestBytes() {
        Random random = new Random(42);
        byte[] bytes = new byte[769557];
        random.nextBytes(bytes);
        return bytes;
    }

}
