package de.mherrmann.tomatofilebackup.core.cli;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Optional;

public class BackupProgressTest {

    private final PrintStream standardOut = System.out;
    private final PrintStream standardErr = System.err;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errStreamCaptor = new ByteArrayOutputStream();

    @BeforeEach
    public void setUp() {
        System.setOut(new PrintStream(outputStreamCaptor));
        System.setErr(new PrintStream(errStreamCaptor));
        CommandLineInterface.test = true;
    }

    @AfterEach
    public void tearDown() {
        System.setOut(standardOut);
        System.setErr(standardErr);
        CommandLineInterface.test = false;
    }

    @Test
    void shouldSetGoal(){
        String indicator = "Finished";
        String pattern = "^Finished.*/\\s*42 Bytes \\(.*%\\)$";

        BackupProgress.setGoal(42);

        assertValidPrinted(indicator, pattern);
    }

    @Test
    void shouldRegisterChunking(){
        String testPath = "/test/chunking/path";
        String indicator = "Chunking";
        String pattern = "^ Chunking "+testPath+"$";
        String pattern2 = "^.*/\\s*23 Bytes \\(.*%\\)$";

        BackupProgress.registerChunking(testPath, 23);

        assertValidPrinted(indicator, pattern, Optional.of(pattern2));
    }

    @Test
    void shouldRegisterTransfer(){
        String testPath = "/test/transfer/path";
        String indicator = "Transfer";
        String pattern = "^ Transfer "+testPath+"$";
        String pattern2 = "^.*/\\s*15 Bytes \\(.*%\\)$";

        BackupProgress.registerTransfer(testPath, 15);

        assertValidPrinted(indicator, pattern, Optional.of(pattern2));
    }

    @Test
    void shouldUpdateChunkingProgress(){
        String testPath = "/test/chunking/path";
        String indicator = "Chunking";
        String pattern = "^ Chunking "+testPath+"$";
        String pattern2 = "^.*13 Bytes /\\s*77 Bytes \\(.*%\\)$";
        BackupProgress.registerChunking(testPath, 77);
        outputStreamCaptor.reset();

        BackupProgress.updateChunkingProgress(13);

        assertValidPrinted(indicator, pattern, Optional.of(pattern2));
    }

    @Test
    void shouldUpdateTransferProgress(){
        String testPath = "/test/transfer/path";
        String indicator = "Transfer";
        String pattern = "^ Transfer "+testPath+"$";
        String pattern2 = "^.*19 Bytes /\\s*99 Bytes \\(.*%\\)$";
        BackupProgress.registerTransfer(testPath, 99);
        outputStreamCaptor.reset();

        BackupProgress.updateTransferProgress(19);

        assertValidPrinted(indicator, pattern, Optional.of(pattern2));
    }

    private void assertValidPrinted(String indicator, String pattern, Optional<String> pattern2){
        String[] lines = outputStreamCaptor.toString().split("\n");
        for(int i = 0; i < lines.length; i++){
            String line = lines[i];
            if(line.contains(indicator)){
                assertTrue(line.matches(pattern));
                if(pattern2.isPresent()){
                    assertTrue(lines[i+1].matches(pattern2.get()));
                }
            }
        }
    }

    private void assertValidPrinted(String indicator, String pattern){
        assertValidPrinted(indicator, pattern, Optional.empty());
    }
}
