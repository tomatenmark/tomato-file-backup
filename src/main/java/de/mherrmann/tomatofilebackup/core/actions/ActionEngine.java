package de.mherrmann.tomatofilebackup.core.actions;

import java.util.*;

public abstract class ActionEngine {



    public abstract void run(Map<String, String> properties, List<String> enabledSwitches, List<String> mainValues) throws Exception;

    public abstract String getActionHelpMessage();

}
