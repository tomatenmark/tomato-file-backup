package de.mherrmann.tomatofilebackup.filetransfer;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionEngine {

    private CompressionEngine(){}

    public static void storeCompressed(byte[] uncompressed, File targetFile) throws IOException {
        try (
                GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(targetFile))
        ){
            gzipOS.write(uncompressed);
        } catch (IOException exception){
            throw new IOException("Error while compression", exception);
        }
    }

    public static byte[] restoreDecompressed(File sourceFile, int unzippedLength) throws IOException {
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
