package de.mherrmann.tomatofilebackup.core.cli;

public class BackupProgress {

    private static final int LINES = 3;

    private static volatile long totalBytes;
    private static volatile long fileBytes;
    private static volatile long totalBytesProcessed;
    private static volatile long fileBytesChunked;
    private static volatile long fileBytesWritten;
    private static volatile String currentFileChunking;
    private static volatile String currentFileWriting;

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
        return String.format("Running backup...\n" +
                    "Finished %s / %s (%s %%)\n" +
                    " Chunking %s\n" +
                    "   %s / %s (%s %%)\n" +
                    " Storing %s\n" +
                    "   %s / %s (%s %%)\n",
                    ProgressHelper.getFormattedBytes(totalBytesProcessed),
                    ProgressHelper.getFormattedBytes(totalBytes),
                    ProgressHelper.getPercent(totalBytesProcessed, totalBytes),
                    ProgressHelper.getShortPath(currentFileChunking),
                    ProgressHelper.getFormattedBytes(fileBytesChunked),
                    ProgressHelper.getFormattedBytes(fileBytes),
                    ProgressHelper.getPercent(fileBytesChunked, fileBytes),
                    ProgressHelper.getShortPath(currentFileWriting),
                    ProgressHelper.getFormattedBytes(fileBytesWritten),
                    ProgressHelper.getFormattedBytes(fileBytes),
                    ProgressHelper.getPercent(fileBytesWritten, fileBytes)
                );
    }
}
