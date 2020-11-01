package de.mherrmann.tomatofilebackup.core;

public class Option {

    public static String getPropertyText(Property property){
        return "--"+property.name()+"="+property.placeholder+" | " + property.description;
    }

    public static String getSwitchText(Switch aSwitch){
        return "-"+aSwitch.name()+" | " + aSwitch.description;
    }

    public enum Switch {
        d("enable debug mode"),
        p("preserve owners and permissions"),
        h("preserve hardlinks"),
        l("follow symlinks"),
        m("follow mount points");

        private final String description;

        Switch(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public enum Property {
        repository("PATH", "path to repository"),
        include("COMMA_SEPARATED_LIST", "list of paths to include, regular expressions supported"),
        exclude("COMMA_SEPARATED_LIST", "list of paths to exclude, regular expressions supported"),
        includeInFile("PATH", "path to file with list of paths to include, regular expressions supported, one path per line"),
        excludeInFile("PATH", "path to file with list of paths to exclude, regular expressions supported, one path per line"),
        compressTypes("COMMA_SEPARATED_LIST", "list of mime types to compress\n" +
                "    Example: --compressTypes=text/txt,text/html -> text and html files will be compressed"),
        compressTypesInFile("PATH", "path to file with list of mime types to compress, one per line"),
        logFile("PATH", "path to log file, enabled logging");

        private final String placeholder;
        private final String description;

        Property(String placeholder, String description) {
            this.placeholder = placeholder;
            this.description = description;
        }

        public String getPlaceholder() {
            return placeholder;
        }

        public String getDescription() {
            return description;
        }
    }
}
