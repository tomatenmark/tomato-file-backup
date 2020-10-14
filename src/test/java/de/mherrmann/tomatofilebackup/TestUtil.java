package de.mherrmann.tomatofilebackup;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class TestUtil {

    public static byte[] buildReproducibleRandomTestBytes() {
        Random random = new Random(42);
        byte[] bytes = new byte[769557];
        random.nextBytes(bytes);
        return bytes;
    }

    public static byte[] buildBiggerReproducibleRandomTestBytes() {
        Random random = new Random(42);
        byte[] bytes = new byte[2008580];
        random.nextBytes(bytes);
        return bytes;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File buildRandomTestFile(long length) throws Exception {
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
        return file;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File buildTestFileWithZeroChars(int length) throws Exception {
        File file = new File("./test.bin");
        file.createNewFile();
        FileOutputStream outputStream = new FileOutputStream(file);
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) 0x30); //ascii char 0
        outputStream.write(bytes);
        outputStream.close();
        outputStream.flush();
        return file;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void createTestDirectory() {
        File dir = new File("./test");
        if(!dir.exists()){
            dir.mkdir();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void removeTestFiles() {
        File directory = new File("./test");
        if(directory.exists()){
            for (File file : Objects.requireNonNull(directory.listFiles())) {
                file.delete();
            }
            directory.delete();
        }
        new File("./test.bin").delete();
    }
}
