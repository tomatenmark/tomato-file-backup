package de.mherrmann.tomatofilebackup.core.cli;

import de.mherrmann.tomatofilebackup.core.Option;
import de.mherrmann.tomatofilebackup.core.actions.ActionEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Command {

    protected ActionEngine actionEngine;
    protected final Map<Option.Property, String> properties = new HashMap<>();
    protected final List<Option.Switch> enabledSwitches = new ArrayList<>();
    protected final List<String> mainValues = new ArrayList<>();

    public void setActionEngine(ActionEngine actionEngine){
        this.actionEngine = actionEngine;
    }

    public String getActionHelpMessage(){
        return actionEngine.getActionHelpMessage();
    }

    public void addProperty(Option.Property key, String value){
        this.properties.put(key, value);
    }

    public void enableSwitch(Option.Switch switchToEnable){
        if(!this.enabledSwitches.contains(switchToEnable)){
            this.enabledSwitches.add(switchToEnable);
        }
    }

    public void addMainValue(String mainValue) {
        if(!this.mainValues.contains(mainValue)){
            this.mainValues.add(mainValue);
        }
    }

    public void run() throws Exception {
        actionEngine.run(properties, enabledSwitches, mainValues);
    }
}
