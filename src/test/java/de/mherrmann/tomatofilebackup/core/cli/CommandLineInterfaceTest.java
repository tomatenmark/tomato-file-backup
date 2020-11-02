package de.mherrmann.tomatofilebackup.core.cli;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.core.Option;
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

    private static final String PREVIOUS_PROGRESS_LINES = "apple\nbanana";
    private static final String CURRENT_PROGRESS_LINES = "citron\ndewberry";
    private static final String MOVE_UP_2_LINES = "\033[2A";

    @BeforeEach
    public void setUp() {
        System.setOut(new PrintStream(outputStreamCaptor));
        System.setErr(new PrintStream(errStreamCaptor));
    }

    @AfterEach
    public void tearDown() {
        System.setOut(standardOut);
        System.setErr(standardErr);
        CommandLineInterface.test = false;
    }

    @Test
    void shouldReturnTrueForIsHelp(){
        boolean help = CommandLineInterface.isHelp(new String[]{"help"});

        assertTrue(help);
    }

    @Test
    void shouldReturnFalseForIsHelpOtherAction(){
        boolean help = CommandLineInterface.isHelp(new String[]{"other"});

        assertFalse(help);
    }

    @Test
    void shouldReturnFalseForIsHelpNoAction(){
        boolean help = CommandLineInterface.isHelp(new String[]{});

        assertFalse(help);
    }

    @Test
    void shouldShowInitializeActionHelpByArgs(){
        CommandLineInterface.showActionHelp(new String[]{"help", "initialize"});

        assertEquals(new InitializeActionEngine().getActionHelpMessage(), outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldShowInitializeActionHelpByCommand(){
        Command command = new Command();
        InitializeActionEngine engine = new InitializeActionEngine();
        command.setActionEngine(engine);

        CommandLineInterface.showActionHelp(command);

        assertEquals(engine.getActionHelpMessage(), outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldShowGeneralHelp(){
        CommandLineInterface.showGeneralHelp();

        assertTrue(outputStreamCaptor.toString().trim().startsWith(Constants.TFB_INTRO+"\n Usage: tfb ACTION [OPTIONS]"));
    }

    @Test
    void shouldPrintMessageToStdOut(){
        String testMessage = "testMessage";

        CommandLineInterface.stdOut("testMessage");

        assertEquals(testMessage, outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldPrintMessageToStdErr(){
        String testMessage = "testMessage";

        CommandLineInterface.stdErr("testMessage");

        assertEquals(testMessage, errStreamCaptor.toString().trim());
    }

    @Test
    void shouldParseArgs(){
        Map<Option.Property, String> expectedProperties = new HashMap<>();
        expectedProperties.put(Option.Property.repository, "testPath");

        Command command = CommandLineInterface.parseArgs(new String[]{"initialize", "-d", "-h", "-l", "--repository=testPath", "test1", "test2"});

        assertEquals(InitializeActionEngine.class, command.actionEngine.getClass());
        assertIterableEquals(Arrays.asList(Option.Switch.d, Option.Switch.h, Option.Switch.l), command.enabledSwitches);
        assertIterableEquals(expectedProperties.keySet(), command.properties.keySet());
        assertIterableEquals(expectedProperties.values(), command.properties.values());
        assertLinesMatch(Arrays.asList("test1", "test2"), command.mainValues);
    }

    @Test
    void shouldFailParseArgsCausedByNoAction(){
        Exception exception = null;

        try {
            CommandLineInterface.parseArgs(new String[]{});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.MISSING_ACTION.getMessage(), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidAction(){
        Exception exception = null;

        try {
            CommandLineInterface.parseArgs(new String[]{"invalid"});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_ACTION.getMessage("invalid"), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidArgumentSingleDash(){
        Exception exception = null;

        try {
            CommandLineInterface.parseArgs(new String[]{"initialize", "-"});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_OPTION.getMessage("-"), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidArgumentMalformedProperty(){
        Exception exception = null;
        String invalidArgument = "--prop";

        try {
            CommandLineInterface.parseArgs(new String[]{"initialize", invalidArgument});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_OPTION.getMessage(invalidArgument), exception.getMessage());
    }

    @Test
    void shouldFailParseArgsCausedByInvalidArgumentInvalidProperty(){
        Exception exception = null;
        String invalidArgument = "--invalid=value";

        try {
            CommandLineInterface.parseArgs(new String[]{"initialize", invalidArgument});
        } catch(Exception ex){
            exception = ex;
        }

        assertNotNull(exception);
        assertEquals(IllegalCommandException.class, exception.getClass());
        assertEquals(Constants.ErrorReport.INVALID_OPTION.getMessage(invalidArgument), exception.getMessage());
    }

    @Test
    void shouldShowOldAndNewProgressLinesAndAnsiSequenceBetween(){
        CommandLineInterface.test = true;

        System.out.print("\n"+PREVIOUS_PROGRESS_LINES);
        String expected = String.format("\n%s%s\n%s", PREVIOUS_PROGRESS_LINES, MOVE_UP_2_LINES, CURRENT_PROGRESS_LINES);

        CommandLineInterface.showProgress(CURRENT_PROGRESS_LINES, 2);

        String actual = outputStreamCaptor.toString();
        assertEquals(expected, actual);
    }

}
