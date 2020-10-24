package de.mherrmann.tomatofilebackup.core;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.exceptions.IllegalActionCommandException;
import de.mherrmann.tomatofilebackup.exceptions.IllegalCommandException;

public class Application {

    public static void run(String[] args){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        if(commandLineInterface.isHelp(args)){
            showHelp(commandLineInterface, args);
        } else {
            processCommand(args, commandLineInterface);
        }
    }

    private static void showHelp(CommandLineInterface commandLineInterface, String[] args){
        try {
            commandLineInterface.showActionHelp(args);
        } catch (IllegalCommandException exception){
            handleIllegalCommandException(commandLineInterface, exception);
        }
    }

    private static void processCommand(String[] args, CommandLineInterface commandLineInterface){
        Command command = null;
        try {
            command = commandLineInterface.parseArgs(args);
            command.run();
            commandLineInterface.stdOut(Constants.SUCCESS);
        } catch (IllegalActionCommandException exception){
            handleIllegalActionCommandException(commandLineInterface, exception, command);
        } catch (IllegalCommandException exception){
            handleIllegalCommandException(commandLineInterface, exception);
        } catch (Exception exception){
            handleException(commandLineInterface, exception);
        }
    }

    private static void handleIllegalActionCommandException(CommandLineInterface commandLineInterface, IllegalActionCommandException exception, Command command){
        commandLineInterface.stdErr(exception.getMessage());
        if(command != null){
            commandLineInterface.showActionHelp(command);
        }
    }

    private static void handleIllegalCommandException(CommandLineInterface commandLineInterface, IllegalCommandException exception){
        commandLineInterface.stdErr(exception.getMessage());
        commandLineInterface.showGeneralHelp();
    }

    private static void handleException(CommandLineInterface commandLineInterface, Exception exception){
        commandLineInterface.stdErr(exception.getMessage());
        exception.printStackTrace();
    }

}
