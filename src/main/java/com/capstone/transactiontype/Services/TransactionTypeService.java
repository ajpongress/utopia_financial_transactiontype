package com.capstone.transactiontype.Services;

import com.capstone.transactiontype.Configurations.BatchConfigAllTypes;
import com.capstone.transactiontype.Configurations.BatchConfigSingleType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.NoSuchElementException;

@Service
public class TransactionTypeService {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    BatchConfigAllTypes batchConfigAllTypes;

    @Autowired
    BatchConfigSingleType batchConfigSingleType;

    private JobParameters buildJobParameters_AllTypes(String pathInput, String pathOutput) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addString("file.input", pathInput)
                .addString("outputPath_param", pathOutput)
                .toJobParameters();
    }

    private JobParameters buildJobParameters_SingleType(String typeID, String pathInput, String pathOutput) {

        // Check if source file.input is valid
        File file = new File(pathInput);
        if (!file.exists()) {
            throw new ItemStreamException("Requested source doesn't exist");
        }

        return new JobParametersBuilder()
                .addString("typeID_param", typeID)
                .addString("file.input", pathInput)
                .addString("outputPath_param", pathOutput)
                .toJobParameters();
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    // Export all types
    public ResponseEntity<String> exportAllTypes(String pathInput, String pathOutput) {

        try {
            JobParameters jobParameters = buildJobParameters_AllTypes(pathInput, pathOutput);
            jobLauncher.run(batchConfigAllTypes.job_exportAllTypes(), jobParameters);

        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }


    // Export a specific type
    public ResponseEntity<String> exportSingleType(String typeID, String pathInput, String pathOutput) {

        try {
            JobParameters jobParameters = buildJobParameters_SingleType(typeID, pathInput, pathOutput);
            jobLauncher.run(batchConfigSingleType.job_exportSingleType(), jobParameters);

        } catch (NumberFormatException e) {
            return new ResponseEntity<>("Type ID format invalid", HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>("Requested source doesn't exist", HttpStatus.BAD_REQUEST);
        } catch (JobExecutionAlreadyRunningException e) {
            return new ResponseEntity<>("Job execution already running", HttpStatus.BAD_REQUEST);
        } catch (JobRestartException e) {
            return new ResponseEntity<>("Job restart exception", HttpStatus.BAD_REQUEST);
        } catch (JobInstanceAlreadyCompleteException e) {
            return new ResponseEntity<>("Job already completed", HttpStatus.BAD_REQUEST);
        } catch (JobParametersInvalidException e) {
            return new ResponseEntity<>("Job parameters are invalid", HttpStatus.BAD_REQUEST);
        }

        // Job successfully ran
        return new ResponseEntity<>("Job parameters OK. Job Completed", HttpStatus.CREATED);
    }
}
