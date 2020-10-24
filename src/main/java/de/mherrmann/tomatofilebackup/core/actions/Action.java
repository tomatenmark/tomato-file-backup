package de.mherrmann.tomatofilebackup.core.actions;

public enum Action {
    initialize(new InitializeActionEngine());
    /*backup(null),
    restore(null),
    list(null),
    mount(null),
    find(null),
    remove(null),
    prune(null),
    check(null);*/

    private final ActionEngine actionEngine;

    Action(ActionEngine actionEngine){
        this.actionEngine = actionEngine;
    }

    public ActionEngine getAction() {
        return actionEngine;
    }
}
