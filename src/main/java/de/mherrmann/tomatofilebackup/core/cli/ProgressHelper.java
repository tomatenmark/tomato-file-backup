package de.mherrmann.tomatofilebackup.core.cli;

import java.io.File;

public class ProgressHelper {

    private ProgressHelper(){}

    public static String getFormattedBytes(long bytes){
        double size = bytes;
        String[] unit = new String[]{"Bytes", "KB", "MB", "GB", "TB"};
        int unitC = 0;
        while( size > 1024 && unitC < unit.length )
        {
            size/=1024;
            unitC++;
        }
        String formattedDoubleStr = ""+size;
        if( unitC > 0 ){
            formattedDoubleStr = new java.text.DecimalFormat("0.00").format( size );
        }
        return String.format("%5s %5s", formattedDoubleStr, unit[unitC]);
    }

    public static int getPercent(long current, long total){
        if(current < total){
            return (int) (total / 100 * current);
        }
        return 100;
    }

    public static String getShortPath(String path){
        if(path.length() > 60){
            path = "..."+File.separator+ new File(path).getName();
        }
        if(path.length() > 60){
            path = path.replaceFirst("^(.{30}).*(.{30})$", "$1...$2");
        }
        return path;
    }
}
