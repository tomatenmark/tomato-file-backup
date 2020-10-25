package de.mherrmann.tomatofilebackup;

public class Constants {
    public static final String DB_FILENAME = "repository.db";
    public static final String VERSION = "1.0";
    public static final String TFB_INTRO = "tomato file backup | Version " + VERSION;
    public static final String HELP = "help";
    public static final String SUCCESS = "Successfully done the action";

    private static final String ERROR = "Error: ";

    public enum ErrorReport {
        REPOSITORY_ALREADY_EXISTS("Repository already exists."),
        PARENT_DIRECTORY_PROBLEM("The parent directory from given path was not found, is not a directory or is not writable: %s"),
        PATH_MUST_BE_DIRECTORY("Given path must be directory."),
        MISSING_ACTION("Missing action. Please give name of action you want help for."),
        INVALID_ACTION("There is no such action: %s"),
        MISSING_PATH("Missing path argument"),
        TOO_FEW_ARGUMENTS("There must be at least one argument, the name of the action."),
        INVALID_ARGUMENT("Invalid Argument: %s");

        private final String message;

        ErrorReport(String message){
            this.message = ERROR + message;
        }

        public String getMessage() {
            return message;
        }

        public String getMessage(String value) {
            return String.format(message, value);
        }
    }
}
