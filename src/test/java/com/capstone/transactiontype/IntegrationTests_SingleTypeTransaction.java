package com.capstone.transactiontype;


import com.capstone.transactiontype.Classifiers.TransactionTypeClassifier;
import com.capstone.transactiontype.Configurations.BatchConfigSingleType;
import com.capstone.transactiontype.Models.TransactionTypeModel;
import com.capstone.transactiontype.Processors.SingleTypeProcessor;
import com.capstone.transactiontype.Readers.TransactionTypeReaderCSV;
import com.capstone.transactiontype.TaskExecutors.TaskExecutor;
import com.capstone.transactiontype.Writers.TransactionTypeCompositeWriter;
import org.apache.commons.io.FileUtils;
import org.aspectj.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.File;

// ********************************************************************************
//                          Test Single Type Operations
// ********************************************************************************

@SpringBatchTest
@SpringJUnitConfig(classes = {
        BatchConfigSingleType.class,
        TransactionTypeClassifier.class,
        TransactionTypeModel.class,
        TransactionTypeReaderCSV.class,
        SingleTypeProcessor.class,
        TransactionTypeCompositeWriter.class,
        TaskExecutor.class
})
@EnableAutoConfiguration

public class IntegrationTests_SingleTypeTransaction {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    // Set typeID to test for single type operations & export
    private String typeID = "Online Transaction";
    private String INPUT = "src/test/resources/input/test_input.csv";
    private String EXPECTED_OUTPUT = "src/test/resources/output/expected_output_SingleTypeTransaction.xml";
    private String ACTUAL_OUTPUT = "src/test/resources/output/type_" + typeID;

//    @BeforeEach
//    public void setup(@Autowired Job job_singleUser) {
//        jobLauncherTestUtils.setJob(job_singleUser);
//    }

    @AfterEach
    public void cleanUp() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    private JobParameters testJobParameters_SingleType() {

        return new JobParametersBuilder()
                .addString("typeID_param", typeID)
                .addString("file.input", INPUT)
                .addString("outputPath_param", ACTUAL_OUTPUT)
                .toJobParameters();
    }


    // ----------------------------------------------------------------------------------
    // --                                 TESTS                                        --
    // ----------------------------------------------------------------------------------

    @Test
    public void testBatchProcessFor_SingleType() throws Exception {

        // Load job parameters and launch job through test suite
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters_SingleType());
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();

        // ----- Assertions -----
        File testInputFile = new File(INPUT);
        File testOutputFileExpected = new File(EXPECTED_OUTPUT);
        File testOutputFileActual = new File(ACTUAL_OUTPUT + "/type_" + typeID + "_transactions.xml");

        // Match job names
        Assertions.assertEquals("exportSingleTypeJob", actualJobInstance.getJobName());

        // Match job exit status to "COMPLETED"
        Assertions.assertEquals("COMPLETED", actualJobExitStatus.getExitCode());

        // Verify input file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testInputFile));

        // Verify output (expected) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileExpected));

        // Verify output (actual) file is valid and can be read
        Assertions.assertTrue(FileUtil.canReadFile(testOutputFileActual));

        // Verify expected and actual output files match
        Assertions.assertEquals(
                FileUtils.readFileToString(testOutputFileExpected, "utf-8"),
                FileUtils.readFileToString(testOutputFileActual, "utf-8"),
                "============================== FILE MISMATCH ==============================");

    }
}

