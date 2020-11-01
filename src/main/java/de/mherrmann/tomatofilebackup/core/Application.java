package de.mherrmann.tomatofilebackup.core;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.core.cli.Command;
import de.mherrmann.tomatofilebackup.core.cli.CommandLineInterface;
import de.mherrmann.tomatofilebackup.exceptions.IllegalActionCommandException;
import de.mherrmann.tomatofilebackup.exceptions.IllegalCommandException;

public class Application {

    public static void run(String[] args){
        CommandLineInterface.init();
        if(CommandLineInterface.isHelp(args)){
            showHelp(args);
        } else {
            processCommand(args);
        }
        CommandLineInterface.finalise();
    }

    private static void showHelp(String[] args){
        try {
            CommandLineInterface.showActionHelp(args);
        } catch (IllegalCommandException exception){
            handleIllegalCommandException(exception);
        }
    }

    private static void processCommand(String[] args){
        Command command = null;
        try {
            command = CommandLineInterface.parseArgs(args);
            command.run();
            CommandLineInterface.stdOut(Constants.SUCCESS);
        } catch (IllegalActionCommandException exception){
            handleIllegalActionCommandException(exception, command);
        } catch (IllegalCommandException exception){
            handleIllegalCommandException(exception);
        } catch (Exception exception){
            handleException(exception);
        }
    }

    private static void handleIllegalActionCommandException(IllegalActionCommandException exception, Command command){
        CommandLineInterface.stdErr(exception.getMessage());
        if(command != null){
            CommandLineInterface.showActionHelp(command);
        }
    }

    private static void handleIllegalCommandException(IllegalCommandException exception){
        CommandLineInterface.stdErr(exception.getMessage());
        CommandLineInterface.showGeneralHelp();
    }

    private static void handleException(Exception exception){
        CommandLineInterface.stdErr(exception.getMessage());
        if(CommandLineInterface.isDebug()){
            exception.printStackTrace();
        }
    }

}
