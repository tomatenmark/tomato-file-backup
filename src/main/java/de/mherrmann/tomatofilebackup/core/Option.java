package de.mherrmann.tomatofilebackup.core;

public enum Option {
    repository(true);

    private boolean switchType;
    private boolean propertyType;

    Option(boolean propertyType) {
        this.switchType = !propertyType;
        this.propertyType = propertyType;
    }
}
