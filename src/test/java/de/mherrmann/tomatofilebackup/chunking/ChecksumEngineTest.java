package de.mherrmann.tomatofilebackup.chunking;

import static org.junit.jupiter.api.Assertions.*;

import de.mherrmann.tomatofilebackup.TestUtil;
import org.junit.jupiter.api.Test;

class ChecksumEngineTest {

    @Test
    void shouldGetChecksumFromFixedMessage(){
        byte[] bytes = "HASH".getBytes();

        String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);

        assertEquals("4341BCFB28F64BED93ACA9A378AE7D8C", checksum);
    }

    @Test
    void shouldGetChecksumFromReproducibleRandomBytes(){
        byte[] bytes = TestUtil.buildReproducibleRandomTestBytes();

        String checksum = ChecksumEngine.getChecksum(bytes, 0, bytes.length);

        assertEquals("BD09C7FCA1866E2CA6FB601FB9E9626F", checksum);
    }

}
