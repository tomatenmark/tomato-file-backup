package de.mherrmann.tomatofilebackup.core;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.core.actions.InitializeActionEngine;
import de.mherrmann.tomatofilebackup.exceptions.IllegalCommandException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CommandLineInterfaceTest {

    private final PrintStream standardOut = System.out;
    private final PrintStream standardErr = System.err;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errStreamCaptor = new ByteArrayOutputStream();

    @BeforeEach
    public void setUp() {
        System.setOut(new PrintStream(outputStreamCaptor));
        System.setErr(new PrintStream(errStreamCaptor));
    }

    @AfterEach
    public void tearDown() {
        System.setOut(standardOut);
        System.setErr(standardErr);
    }

    @Test
    void shouldReturnTrueForIsHelp(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();

        boolean help = commandLineInterface.isHelp(new String[]{"help"});

        assertTrue(help);
    }

    @Test
    void shouldReturnFalseForIsHelpOtherAction(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();

        boolean help = commandLineInterface.isHelp(new String[]{"other"});

        assertFalse(help);
    }

    @Test
    void shouldReturnFalseForIsHelpNoAction(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();

        boolean help = commandLineInterface.isHelp(new String[]{});

        assertFalse(help);
    }

    @Test
    void shouldShowInitializeActionHelpByArgs(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();

        commandLineInterface.showActionHelp(new String[]{"help", "initialize"});

        assertEquals(new InitializeActionEngine().getActionHelpMessage(), outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldShowInitializeActionHelpByActionEngine(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        InitializeActionEngine engine = new InitializeActionEngine();

        commandLineInterface.showActionHelp(engine);

        assertEquals(engine.getActionHelpMessage(), outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldShowInitializeActionHelpByCommand(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Command command = new Command();
        InitializeActionEngine engine = new InitializeActionEngine();
        command.setActionEngine(engine);

        commandLineInterface.showActionHelp(command);

        assertEquals(engine.getActionHelpMessage(), outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldShowGeneralHelp(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();

        commandLineInterface.showGeneralHelp();

        assertTrue(outputStreamCaptor.toString().trim().startsWith(Constants.TFB_INTRO+"\n Usage: tfb ACTION [OPTIONS]"));
    }

    @Test
    void shouldPrintMessageToStdOut(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        String testMessage = "testMessage";

        commandLineInterface.stdOut("testMessage");

        assertEquals(testMessage, outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldPrintMessageToStdErr(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        String testMessage = "testMessage";

        commandLineInterface.stdErr("testMessage");

        assertEquals(testMessage, errStreamCaptor.toString().trim());
    }

    @Test
    void shouldParseArgs(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Map<Option.Property, String> expectedProperties = new HashMap<>();
        expectedProperties.put(Option.Property.repository, "testPath");

        //-d -> enables debug mode and is not added to enabled switches
        Command command = commandLineInterface.parseArgs(new String[]{"initialize", "-d", "-h", "-lv", "--repository=testPath", "test1", "test2"});

        assertEquals(InitializeActionEngine.class, command.actionEngine.getClass());
        assertIterableEquals(Arrays.asList(Option.Switch.h, Option.Switch.l, Option.Switch.v), command.enabledSwitches);
        assertIterableEquals(expectedProperties.keySet(), command.properties.keySet());
        assertIterableEquals(expectedProperties.values(), command.properties.values());
        assertLinesMatch(Arrays.asList("test1", "test2"), command.mainValues);
    }

    @Test
    void shouldFailParseArgsCausedByNoAction(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Exception exception = null;

        try {
            commandLineInterface.parseArgs(new String[]{});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.TOO_FEW_ARGUMENTS.getMessage(), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidAction(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Exception exception = null;

        try {
            commandLineInterface.parseArgs(new String[]{"invalid"});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_ACTION.getMessage("invalid"), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidArgumentSingleDash(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Exception exception = null;

        try {
            commandLineInterface.parseArgs(new String[]{"initialize", "-"});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_ARGUMENT.getMessage("-"), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidArgumentMalformedProperty(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Exception exception = null;
        String invalidArgument = "--prop";

        try {
            commandLineInterface.parseArgs(new String[]{"initialize", invalidArgument});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_ARGUMENT.getMessage(invalidArgument), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidArgumentInvalidProperty(){
        CommandLineInterface commandLineInterface = new CommandLineInterface();
        Exception exception = null;
        String invalidArgument = "--invalid=value";

        try {
            commandLineInterface.parseArgs(new String[]{"initialize", invalidArgument});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_ARGUMENT.getMessage(invalidArgument), exception.getMessage());
    }

}
