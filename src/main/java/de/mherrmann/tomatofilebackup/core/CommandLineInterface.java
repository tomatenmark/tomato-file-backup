package de.mherrmann.tomatofilebackup.core;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.core.actions.Action;
import de.mherrmann.tomatofilebackup.core.actions.ActionEngine;
import de.mherrmann.tomatofilebackup.exceptions.IllegalCommandException;

public class CommandLineInterface {

    private static boolean debug;

    private CommandLineInterface(){}

    static boolean isHelp(String[] args){
        return args.length > 0 && args[0].equals(Constants.HELP);
    }

    static boolean isDebug(){
        return debug;
    }

    static Command parseArgs(String[] args){
        if(args.length == 0){
            throw new IllegalCommandException(Constants.ErrorReport.TOO_FEW_ARGUMENTS.getMessage());
        }
        String action = args[0];
        Command command = new Command();
        try {
            command.setActionEngine(getActionEngine(action));
        } catch(IllegalArgumentException exception){
            throw new IllegalCommandException(Constants.ErrorReport.INVALID_ACTION.getMessage(action));
        }

        parseOptions(args, command);
        return command;
    }

    static void showGeneralHelp(){
        StringBuilder help = new StringBuilder();
        help.append(Constants.TFB_INTRO).append("\n");
        help.append(" Usage: tfb ACTION [OPTIONS]\n");
        help.append("  Example: tfb backup --repository=/mnt/backup/repo/ -n /home/max/\n");
        help.append("\n");
        help.append(" Execute tfb help ACTION for action specific help\n");
        help.append("  Example: tfb help backup\n");
        help.append("\n");
        help.append(" Actions: ");
        boolean first = true;
        for (Action action : Action.values()){
            if(!first){
                help.append(", ");
            }
            help.append(action.name());
            first = false;
        }
        stdOut(help.toString());
    }

    static void showActionHelp(String[] args){
        if(args.length < 2){
            throw new IllegalCommandException(Constants.ErrorReport.MISSING_ACTION.getMessage());
        }
        String action = args[1];
        try {
            ActionEngine actionEngine = Action.valueOf(action).getAction();
            showActionHelp(actionEngine);
        } catch(IllegalArgumentException exception){
            throw new IllegalCommandException(Constants.ErrorReport.INVALID_ACTION.getMessage(action));
        }
    }

    static void showActionHelp(Command command){
        stdOut(command.getActionHelpMessage());
    }

    static void showActionHelp(ActionEngine actionEngine){
        stdOut(actionEngine.getActionHelpMessage());
    }

    static public void stdOut(String message){
        System.out.println(message);
    }

    static void stdErr(String message){
        System.err.println(message);
    }

    private static ActionEngine getActionEngine(String action){
        return Action.valueOf(action).getAction();
    }

    private static void parseOptions(String[] args, Command command){
        for(int i = 1; i < args.length; i++){
            String arg = args[i];
            if(arg.startsWith("--")){
                addProperty(arg, command);
            } else if(arg.startsWith("-")){
                parseSwitches(arg, command);
            } else {
                command.addMainValue(arg);
            }
        }
    }

    private static void addProperty(String arg, Command command){
        String propertyParsePattern = "^--([^=]+)=([^=]+)$";
        if(!arg.matches(propertyParsePattern)){
            throw new IllegalCommandException(Constants.ErrorReport.INVALID_ARGUMENT.getMessage(arg));
        }
        String key = arg.replaceFirst(propertyParsePattern, "$1");
        try {
            String value = arg.replaceFirst(propertyParsePattern, "$2");
            Option.Property property = Option.Property.valueOf(key);
            command.addProperty(property, value);
        } catch(IllegalArgumentException exception) {
            throw new IllegalCommandException(Constants.ErrorReport.INVALID_ARGUMENT.getMessage(arg));
        }
    }

    private static void parseSwitches(String arg, Command command){
        if(arg.length() == 1){
            throw new IllegalCommandException(Constants.ErrorReport.INVALID_ARGUMENT.getMessage(arg));
        }
        for(int i = 1; i < arg.length(); i++){
            try {
                Option.Switch switchOption = Option.Switch.valueOf(String.valueOf(arg.charAt(i)));
                if(Option.Switch.d.equals(switchOption)){
                    debug = true;
                }
                command.enableSwitch(switchOption);
            } catch(IllegalArgumentException exception){
                throw new IllegalCommandException(Constants.ErrorReport.INVALID_ARGUMENT.getMessage(arg));
            }
        }
    }
}
