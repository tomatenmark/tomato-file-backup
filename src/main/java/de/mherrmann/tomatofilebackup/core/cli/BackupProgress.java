package de.mherrmann.tomatofilebackup.core.cli;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BackupProgress {

    private static final int LINES = 6;

    private static final AtomicBoolean goalSet = new AtomicBoolean(false);
    private static final AtomicBoolean chunkingRegistered = new AtomicBoolean(false);
    private static final AtomicBoolean transferRegistered = new AtomicBoolean(false);
    private static final AtomicLong totalBytes = new AtomicLong();
    private static final AtomicLong totalBytesTransferred = new AtomicLong();
    private static final AtomicLong totalBytesChunked = new AtomicLong();
    private static final AtomicLong fileBytesToTransfer = new AtomicLong();
    private static final AtomicLong fileBytesToChunk = new AtomicLong();
    private static final AtomicLong fileBytesChunked = new AtomicLong();
    private static final AtomicLong fileBytesTransferred = new AtomicLong();
    private static volatile String currentFileChunking = "";
    private static volatile String currentFileTransfer = "";

    private BackupProgress(){}

    public static void setGoal(long bytes){
        goalSet.set(true);
        totalBytes.set(bytes);
        totalBytesTransferred.set(0);
        showProgress();
    }

    public static void registerChunking(String path, long bytes){
        chunkingRegistered.set(true);
        currentFileChunking = path;
        fileBytesToChunk.set(bytes);
        fileBytesChunked.set(0);
        showProgress();
    }

    public static void registerTransfer(String path, long bytes){
        transferRegistered.set(true);
        currentFileTransfer = path;
        fileBytesToTransfer.set(bytes);
        fileBytesTransferred.set(0);
        showProgress();
    }

    public static void updateChunkingProgress(long bytesIncrement){
        fileBytesChunked.addAndGet(bytesIncrement);
        totalBytesChunked.addAndGet(bytesIncrement);
        if(totalBytesChunked.get() >= totalBytes.get()){
            chunkingRegistered.set(false);
        }
        showProgress();
    }

    public static void updateTransferProgress(long bytesIncrement){
        fileBytesTransferred.addAndGet(bytesIncrement);
        totalBytesTransferred.addAndGet(bytesIncrement);
        showProgress();
    }

    private static void showProgress(){
        init();
        String progressString = buildProgressLines();
        CommandLineInterface.showProgress(progressString, LINES);
        if(totalBytesTransferred.get() >= totalBytes.get()){
            reset();
        }
    }

    private static String buildProgressLines() {
        StringBuilder progress = new StringBuilder();
        progress.append("Running backup...\n");
        appendTotalProgress(progress);
        appendChunkingProgress(progress);
        appendStoringProgress(progress);
        return progress.toString();
    }

    private static void appendTotalProgress(StringBuilder progress) {
        if(!goalSet.get()){
            progress.append(String.format(
                    "Finished %s /     ... Bytes (... %%)\n",
                    ProgressHelper.getFormattedBytes(totalBytesTransferred.get())
            ));
            return;
        }
        progress.append(String.format(
                "Finished %s / %s (%s %%)\n",
                ProgressHelper.getFormattedBytes(totalBytesTransferred.get()),
                ProgressHelper.getFormattedBytes(totalBytes.get()),
                ProgressHelper.getFormattedPercent(totalBytesTransferred.get(), totalBytes.get())
        ));
    }

    private static void appendChunkingProgress(StringBuilder progress) {
        if(currentFileChunking.isEmpty()){
            progress.append(" Chunking ...\n   ...\n");
            return;
        }
        progress.append(String.format(
                " Chunking %s\n   %s / %s (%s %%)\n",
                ProgressHelper.getShortPath(currentFileChunking),
                ProgressHelper.getFormattedBytes(fileBytesChunked.get()),
                ProgressHelper.getFormattedBytes(fileBytesToChunk.get()),
                ProgressHelper.getFormattedPercent(fileBytesChunked.get(), fileBytesToChunk.get())
        ));
    }

    private static void appendStoringProgress(StringBuilder progress) {
        if(currentFileTransfer.isEmpty()){
            progress.append(" Transfer ...\n   ...\n");
            return;
        }
        progress.append(String.format(
                " Transfer %s\n   %s / %s (%s %%)\n",
                ProgressHelper.getShortPath(currentFileTransfer),
                ProgressHelper.getFormattedBytes(fileBytesTransferred.get()),
                ProgressHelper.getFormattedBytes(fileBytesToTransfer.get()),
                ProgressHelper.getFormattedPercent(fileBytesTransferred.get(), fileBytesToTransfer.get())
        ));
    }

    private static void init(){
        CommandLineInterface.initProgress(LINES);
    }

    private static void reset(){
        CommandLineInterface.resetProgress(LINES);
    }
}
