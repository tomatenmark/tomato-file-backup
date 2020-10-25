package de.mherrmann.tomatofilebackup.core;

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

    void setActionEngine(ActionEngine actionEngine){
        this.actionEngine = actionEngine;
    }

    void addProperty(Option.Property key, String value){
        this.properties.put(key, value);
    }

    void enableSwitch(Option.Switch switchToEnable){
        if(!this.enabledSwitches.contains(switchToEnable)){
            this.enabledSwitches.add(switchToEnable);
        }
    }

    public void addMainValue(String mainValue) {
        this.mainValues.add(mainValue);
    }

    void run() throws Exception {
        actionEngine.run(properties, enabledSwitches, mainValues);
    }

    String getActionHelpMessage(){
        return actionEngine.getActionHelpMessage();
    }
}
