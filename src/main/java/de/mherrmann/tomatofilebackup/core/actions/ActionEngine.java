package de.mherrmann.tomatofilebackup.core.actions;

import de.mherrmann.tomatofilebackup.core.Option;

import java.util.*;

public abstract class ActionEngine {



    public abstract void run(Map<Option.Property, String> properties, List<Option.Switch> enabledSwitches, List<String> mainValues) throws Exception;

    public abstract String getActionHelpMessage();

}
