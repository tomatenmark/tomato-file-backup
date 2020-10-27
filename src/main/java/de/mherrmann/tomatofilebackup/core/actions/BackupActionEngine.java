package de.mherrmann.tomatofilebackup.core.actions;

import de.mherrmann.tomatofilebackup.core.Option;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BackupActionEngine extends ActionEngine {

    private static final List<Option.Property> AVAILABLE_PROPERTIES = Arrays.asList(
            Option.Property.include, Option.Property.includeInFile, Option.Property.exclude, Option.Property.excludeInFile,
            Option.Property.compressTypes, Option.Property.compressTypesInFile, Option.Property.repository
    );
    private static final List<Option.Switch> AVAILABLE_SWITCHES = Arrays.asList(
            Option.Switch.v, Option.Switch.p, Option.Switch.l, Option.Switch.m
    );
    private static final List<Option.Property> MANDATORY_PROPERTIES = Collections.singletonList(Option.Property.repository);
    private static final String ACTION_NAME = "backup";
    private static final String USAGE = "tfb backup --"+Option.Property.repository.name()+"="+Option.Property.repository.getPlaceholder()+" [OPTIONS] SOURCE_PATH";
    private static final String EXAMPLE = "Example: tfb backup --repository=/mnt/backup/repo/ -p -v /home/max/";

    public BackupActionEngine(){
        super(AVAILABLE_PROPERTIES, AVAILABLE_SWITCHES, MANDATORY_PROPERTIES, ACTION_NAME, USAGE, EXAMPLE);
    }

    @Override
    public void run(Map<Option.Property, String> properties, List<Option.Switch> enabledSwitches, List<String> mainValues) {
        checkBoundaries(properties, enabledSwitches);
    }
}
