package de.mherrmann.tomatofilebackup.core.cli;

public class BackupProgress {

    private static final int LINES = 6;

    private static volatile long totalBytes = -1;
    private static volatile long fileBytes;
    private static volatile long totalBytesProcessed;
    private static volatile long fileBytesChunked;
    private static volatile long fileBytesWritten;
    private static volatile String currentFileChunking = "";
    private static volatile String currentFileWriting = "";

    private BackupProgress(){}

    public static void init(){
        CommandLineInterface.initProgress(LINES);
    }

    public static void reset(){
        CommandLineInterface.resetProgress(LINES);
    }

    public static void setTotalBytes(long totalBytes) {
        BackupProgress.totalBytes = totalBytes;
        showProgress();
    }

    public static void setFileBytes(long fileBytes) {
        BackupProgress.fileBytes = fileBytes;
        showProgress();
    }

    public static void setTotalBytesProcessed(long totalBytesProcessed) {
        BackupProgress.totalBytesProcessed = totalBytesProcessed;
        showProgress();
    }

    public static void setFileBytesChunked(long fileBytesChunked) {
        BackupProgress.fileBytesChunked = fileBytesChunked;
        showProgress();
    }

    public static void setFileBytesWritten(long fileBytesWritten) {
        BackupProgress.fileBytesWritten = fileBytesWritten;
        showProgress();
    }

    public static void setCurrentFileChunking(String currentFileChunking) {
        BackupProgress.currentFileChunking = currentFileChunking;
        showProgress();
    }

    public static void setCurrentFileWriting(String currentFileWriting) {
        BackupProgress.currentFileWriting = currentFileWriting;
        showProgress();
    }

    private static void showProgress(){
        String progressString = buildProgressLines();
        CommandLineInterface.showProgress(progressString, LINES);
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
        if(totalBytes < 0){
            progress.append("Calculating directory size...\n");
            return;
        }
        progress.append(String.format(
                "Finished %s / %s (%s %%)\n",
                ProgressHelper.getFormattedBytes(totalBytesProcessed),
                ProgressHelper.getFormattedBytes(totalBytes),
                ProgressHelper.getFormattedPercent(totalBytesProcessed, totalBytes)
        ));
    }

    private static void appendChunkingProgress(StringBuilder progress) {
        if(currentFileChunking.isEmpty()){
            progress.append(" Wait for Chunking\n   ...\n");
            return;
        }
        progress.append(String.format(
                " Chunking %s\n   %s / %s (%s %%)\n",
                ProgressHelper.getShortPath(currentFileChunking),
                ProgressHelper.getFormattedBytes(fileBytesChunked),
                ProgressHelper.getFormattedBytes(fileBytes),
                ProgressHelper.getFormattedPercent(fileBytesChunked, fileBytes)
        ));
    }

    private static void appendStoringProgress(StringBuilder progress) {
        if(currentFileWriting.isEmpty()){
            progress.append(" Wait for storing\n   ...\n");
            return;
        }
        progress.append(String.format(
                " Storing  %s\n   %s / %s (%s %%)\n",
                ProgressHelper.getShortPath(currentFileWriting),
                ProgressHelper.getFormattedBytes(fileBytesWritten),
                ProgressHelper.getFormattedBytes(fileBytes),
                ProgressHelper.getFormattedPercent(fileBytesWritten, fileBytes)
        ));
    }
}
