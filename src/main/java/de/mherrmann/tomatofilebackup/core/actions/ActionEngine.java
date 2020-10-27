package de.mherrmann.tomatofilebackup.core.actions;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.core.Option;
import de.mherrmann.tomatofilebackup.exceptions.IllegalCommandException;

import java.util.*;

public abstract class ActionEngine {

    private final List<Option.Property> availableProperties;
    private final List<Option.Switch> availableSwitches;
    private final List<Option.Property> mandatoryProperties;
    private final String actionName;
    private final String usage;
    private final String example;

    public ActionEngine(
            List<Option.Property> availableProperties, List<Option.Switch> availableSwitches, List<Option.Property> mandatoryProperties,
            String actionName, String usage, String example) {
        this.availableProperties = availableProperties;
        this.availableSwitches = availableSwitches;
        this.mandatoryProperties = mandatoryProperties;
        this.actionName = actionName;
        this.usage = usage;
        this.example = example;
    }

    public abstract void run(Map<Option.Property, String> properties, List<Option.Switch> enabledSwitches, List<String> mainValues) throws Exception;

    public String getActionHelpMessage() {
        StringBuilder helpText = new StringBuilder();
        appendStaticHelpText(helpText, usage, example);
        if(!availableProperties.isEmpty() || !availableSwitches.isEmpty()){
            helpText.append("\n\n Options:\n");
            for(Option.Property property : availableProperties){
                appendPropertyHelpText(helpText, property);
            }
            for(Option.Switch aSwitch : availableSwitches){
                appendSwitchHelpText(helpText, aSwitch);
            }
        }
        return helpText.toString();
    }

    protected void checkBoundaries(Map<Option.Property, String> properties, List<Option.Switch> enabledSwitches){
        checkMandatoryProperties(properties);
        checkAvailableSwitches(enabledSwitches);
        checkAvailableProperties(properties);
    }

    private void checkMandatoryProperties(Map<Option.Property, String> properties){
        for(Option.Property property : mandatoryProperties){
            if(!properties.containsKey(property)){
                throw new IllegalCommandException(Constants.ErrorReport.MISSING_PROPERTY.getMessage(property));
            }
        }
    }

    private void checkAvailableProperties(Map<Option.Property, String> properties){
        for(Option.Property property : properties.keySet()){
            if(!availableProperties.contains(property)){
                throw new IllegalCommandException(Constants.ErrorReport.INVALID_OPTION.getMessage(property));
            }
        }
    }

    private void checkAvailableSwitches(List<Option.Switch> switches){
        for(Option.Switch aSwitch : switches){
            if(!availableSwitches.contains(aSwitch)){
                throw new IllegalCommandException(Constants.ErrorReport.INVALID_OPTION.getMessage(aSwitch));
            }
        }
    }

    private void appendStaticHelpText(StringBuilder helpText, String usage, String example){
        helpText.append(Constants.TFB_INTRO).append("\n");
        helpText.append("Help for ").append(actionName).append(": \n");
        helpText.append(" Usage: ").append(usage).append("\n");
        helpText.append("  Example: ").append(example);
    }

    private void appendPropertyHelpText(StringBuilder helpText, Option.Property property){
        helpText.append("  ");
        helpText.append(Option.getPropertyText(property));
        helpText.append("\n");
    }

    private void appendSwitchHelpText(StringBuilder helpText, Option.Switch aSwitch){
        helpText.append("  ");
        helpText.append(Option.getSwitchText(aSwitch));
        helpText.append("\n");
    }

}
