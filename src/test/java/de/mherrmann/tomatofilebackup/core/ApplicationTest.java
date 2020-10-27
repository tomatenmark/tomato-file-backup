package de.mherrmann.tomatofilebackup.core;

import de.mherrmann.tomatofilebackup.Constants;
import de.mherrmann.tomatofilebackup.TestUtil;
import de.mherrmann.tomatofilebackup.core.actions.InitializeActionEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class ApplicationTest {

    private static final String TEST_REPOSITORY_PATH = "./test/repo/";

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
        TestUtil.removeTestFiles();
    }

    @Test
    void shouldShowActionHelp(){
        Application.run(new String[]{"help", "initialize"});

        assertEquals(new InitializeActionEngine().getActionHelpMessage(), outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldRunAndPrintSuccess(){
        TestUtil.createTestDirectory();

        Application.run(new String[]{"initialize", TEST_REPOSITORY_PATH});

        assertEquals(Constants.SUCCESS, outputStreamCaptor.toString().trim());
    }

    @Test
    void shouldPrintErrorAndGeneralHelpByNoArgs(){
        Application.run(new String[]{});

        assertEquals(Constants.ErrorReport.MISSING_ACTION.getMessage(), errStreamCaptor.toString().trim());
        assertTrue(outputStreamCaptor.toString().contains("Execute tfb help ACTION for action specific help"));
    }

    @Test
    void shouldPrintErrorAndGeneralHelpByMissingAction(){
        Application.run(new String[]{"help"});

        assertEquals(Constants.ErrorReport.MISSING_ACTION_FOR_HELP.getMessage(), errStreamCaptor.toString().trim());
        assertTrue(outputStreamCaptor.toString().contains("Execute tfb help ACTION for action specific help"));
    }

    @Test
    void shouldPrintErrorAndGeneralHelpByInvalidAction(){
        Application.run(new String[]{"help", "invalid"});

        assertEquals(Constants.ErrorReport.INVALID_ACTION.getMessage("invalid"), errStreamCaptor.toString().trim());
        assertTrue(outputStreamCaptor.toString().contains("Execute tfb help ACTION for action specific help"));
    }

    @Test
    void shouldPrintErrorAndActionHelpByMissingPath(){
        Application.run(new String[]{"initialize"});

        assertEquals(Constants.ErrorReport.MISSING_PATH.getMessage(), errStreamCaptor.toString().trim());
        assertTrue(outputStreamCaptor.toString().contains("Help for initialize"));
    }

    @Test
    void shouldPrintIOError(){
        Application.run(new String[]{"initialize", TEST_REPOSITORY_PATH});

        assertTrue(errStreamCaptor.toString().trim().startsWith(Constants.ErrorReport.PARENT_DIRECTORY_PROBLEM.getMessage("")));
    }

}
