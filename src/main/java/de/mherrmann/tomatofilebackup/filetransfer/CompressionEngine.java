package de.mherrmann.tomatofilebackup.filetransfer;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionEngine {

    private CompressionEngine(){}

    public static void zip(byte[] unzipped, FileOutputStream targetFileStream) throws IOException {
        GZIPOutputStream gzipOS = new GZIPOutputStream(targetFileStream);
        gzipOS.write(unzipped);
        gzipOS.close();
        targetFileStream.close();
    }

    public static byte[] unzip(File sourceFile, int unzippedLength) throws IOException {
        FileInputStream fis = new FileInputStream(sourceFile);
        GZIPInputStream gzipIS = new GZIPInputStream(fis);
        byte[] unzipped = new byte[unzippedLength];
        gzipIS.read(unzipped);
        return unzipped;
    }

}
