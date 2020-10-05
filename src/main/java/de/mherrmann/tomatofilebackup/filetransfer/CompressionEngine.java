package de.mherrmann.tomatofilebackup.filetransfer;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionEngine {

    private CompressionEngine(){}

    public static void storeCompressed(byte[] unzipped, File targetFile) throws IOException {
        long start = 0;
        long end = 0;
        try (
                GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(targetFile))
        ){
            start = System.nanoTime();
            gzipOS.write(unzipped);
            end = System.nanoTime();
        } catch (IOException exception){
            throw new IOException("Error while compression", exception);
        }
        System.out.println(end-start);
    }

    public static byte[] unzip(File sourceFile, int unzippedLength) throws IOException {
        try (
                GZIPInputStream gzipIS = new GZIPInputStream(new FileInputStream(sourceFile))
        ){
            byte[] unzipped = new byte[unzippedLength];
            int read = 0;
            while (read < unzippedLength){
                read += gzipIS.read(unzipped, read, unzippedLength-read);
            }
            return unzipped;
        } catch (IOException exception){
            throw new IOException("Error while decompression", exception);
        }
    }

}
