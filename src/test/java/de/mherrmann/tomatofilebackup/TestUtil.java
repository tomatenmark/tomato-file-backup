package de.mherrmann.tomatofilebackup;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;

public class TestUtil {

    public static byte[] buildReproducibleRandomTestBytes() {
        Random random = new Random(42);
        byte[] bytes = new byte[769557];
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

    public static RandomAccessFile buildRandomTestRandomAccessFile(long length) throws Exception {
        return new RandomAccessFile(buildRandomTestFile(length), "r");
    }
}
