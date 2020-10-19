package de.mherrmann.tomatofilebackup.filetransfer;

import de.mherrmann.tomatofilebackup.chunking.Chunk;
import de.mherrmann.tomatofilebackup.persistence.DatabaseEngine;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class TransferEngine {

    public void storeChunks(File sourceFile, File chunksDirectory,
                            List<Chunk> chunks, boolean compress) throws IOException {
        String chunksDirectoryPath = chunksDirectory.getAbsolutePath();
        try (
                RandomAccessFile sourceRandomAccessFile = new RandomAccessFile(sourceFile, "r");
                FileChannel sourceChannel = sourceRandomAccessFile.getChannel()
        ){
            for (Chunk chunk : chunks) {
                storeChunk(sourceRandomAccessFile, sourceChannel, chunksDirectoryPath, chunk, compress);
            }
        } catch(IOException exception){
            throw new IOException("Error: Could not store chunks", exception);
        }
    }

    public void restoreFile(File targetFile, File chunksDirectory,
                            List<Chunk> chunks, boolean compress) throws IOException {
        String chunksDirectoryPath = chunksDirectory.getAbsolutePath();
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
                restoreChunk(targetRandomAccessFile, targetChannel, chunksDirectoryPath, chunk, compress);
            }
        } catch(IOException exception){
            throw new IOException("Error: Could not restore file", exception);
        }
    }

    public void removeChunkFiles(File chunksDirectory, List<String> checksums) throws IOException, IllegalStateException {
        boolean untouched = true;
        try {
            for(String checksum : checksums){
                File chunkFile = new File(chunksDirectory, checksum);
                Files.delete(chunkFile.toPath());
                untouched = false;
            }
        } catch(IOException exception){
            if(untouched){
                throw new IOException("I/O Error. Could not remove chunk files", exception);
            }
            throw new IllegalStateException("I/O Error. Could not remove all chunk files. Some chunk files where removed.", exception);
        }
    }

    public void removeOrphanedChunkFiles(File chunksDirectory, DatabaseEngine databaseEngine) throws IOException, IllegalStateException {
        try {
            for(File chunkFile : Objects.requireNonNull(chunksDirectory.listFiles())){
                if(!databaseEngine.existsChunkByChecksum(chunkFile.getName())){
                    Files.delete(chunkFile.toPath());
                }
            }
        } catch(IOException | SQLException exception){
            throw new IOException("Error. Could not remove all orphaned chunk files due to i/o error.", exception);
        }
    }

    private void storeChunk(RandomAccessFile source, FileChannel sourceChannel,
                           String chunksDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File chunkFile = new File(chunksDirectoryPath, chunk.getChecksum());
        boolean created = chunkFile.createNewFile();
        if(!created){
            throw new IOException("Unknown Error - Chunk file was not created. Checksum: " + chunk.getChecksum());
        }
        try (
                RandomAccessFile chunkRandomAccessFile = new RandomAccessFile(chunkFile, "rw");
                FileChannel chunkChannel = chunkRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = new byte[chunk.getLength()];
                source.seek(chunk.getOffset());
                source.read(chunkBytes);
                CompressionEngine.storeCompressed(chunkBytes, chunkFile);
            } else {
                sourceChannel.transferTo(chunk.getOffset(), chunk.getLength(), chunkChannel);
            }
        } catch (IOException exception) {
            throw new IOException("Error while storing chunk " + chunk.getChecksum(), exception);
        }
    }

    private void restoreChunk(RandomAccessFile target, FileChannel targetChannel,
                              String chunksDirectoryPath, Chunk chunk, boolean compress) throws IOException {
        File chunkFile = new File(chunksDirectoryPath, chunk.getChecksum());
        try (
                RandomAccessFile chunkRandomAccessFile = new RandomAccessFile(chunkFile, "r");
                FileChannel chunkChannel = chunkRandomAccessFile.getChannel()
        ){
            if(compress){
                byte[] chunkBytes = CompressionEngine.restoreDecompressed(chunkFile, chunk.getLength());
                target.write(chunkBytes);
            } else {
                targetChannel.transferFrom(chunkChannel, chunk.getOffset(), chunk.getLength());
            }
        } catch (IOException exception) {
            throw new IOException("Error while restoring chunk " + chunk.getChecksum(), exception);
        }
    }

}
