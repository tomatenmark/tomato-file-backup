package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;

public class TransferEngine {

    public void storeChunks(File sourceFile, File targetDirectory,
                            List<Chunk> chunks, boolean compress) throws IOException {
        String targetDirectoryPath = targetDirectory.getAbsolutePath();
        try (
                RandomAccessFile sourceRandomAccessFile = new RandomAccessFile(sourceFile, "r");
                FileChannel sourceChannel = sourceRandomAccessFile.getChannel()
        ){
            for (Chunk chunk : chunks) {
                storeChunk(sourceRandomAccessFile, sourceChannel, targetDirectoryPath, chunk, compress);
            }
        } catch(IOException exception){
            throw new IOException("Error: Could not store chunks", exception);
        }
    }

    public void restoreFile(String targetFilePath, File sourceDirectory,
                            List<Chunk> chunks, boolean compress) throws IOException {
        String sourceDirectoryPath = sourceDirectory.getAbsolutePath();
        File targetFile = new File(targetFilePath);
        if(targetFile.exists()){
            Files.delete(targetFile.toPath());
        }
        boolean created = targetFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - File to restore was not created.");
        }
        try (
            RandomAccessFile targetRandomAccessFile = new RandomAccessFile(targetFile, "rw");
            FileChannel targetChannel = targetRandomAccessFile.getChannel()
        ){
            for (Chunk chunk : chunks) {
                restoreChunk(targetRandomAccessFile, targetChannel, sourceDirectoryPath, chunk, compress);
            }
        } catch(IOException exception){
            throw new IOException("Error: Could not restore file", exception);
        }
    }

    private void storeChunk(RandomAccessFile source, FileChannel sourceChannel,
                           String targetDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File targetFile = new File(targetDirectoryPath, chunk.getChecksum());
        boolean created = targetFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile targetRandomAccessFile = new RandomAccessFile(targetFile, "rw");
                FileChannel targetChannel = targetRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = new byte[chunk.getLength()];
                source.seek(chunk.getOffset());
                source.read(chunkBytes);
                CompressionEngine.storeCompressed(chunkBytes, targetFile);
            } else {
                sourceChannel.transferTo(chunk.getOffset(), chunk.getLength(), targetChannel);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

    private void restoreChunk(RandomAccessFile target, FileChannel targetChannel,
                              String sourceDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File sourceFile = new File(sourceDirectoryPath, chunk.getChecksum());
        try (
                RandomAccessFile sourceRandomAccessFile = new RandomAccessFile(sourceFile, "r");
                FileChannel sourceChannel = sourceRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = CompressionEngine.restoreDecompressed(sourceFile, chunk.getLength());
                target.write(chunkBytes);
            } else {
                targetChannel.transferFrom(sourceChannel, chunk.getOffset(), chunk.getLength());
            }
        } catch (IOException exception) {
            throw new IOException("Error while restoring chunk " + chunk.getChecksum(), exception);
        }
    }

}
