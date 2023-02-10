package com.capstone.transactiontype.Configurations;

import com.capstone.transactiontype.Classifiers.TransactionTypeClassifier;
import com.capstone.transactiontype.Models.TransactionTypeModel;
import com.capstone.transactiontype.Processors.SingleTypeProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Slf4j
public class BatchConfigSingleType {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("reader_Transaction_Type")
    private SynchronizedItemStreamReader<TransactionTypeModel> synchronizedItemStreamReader;

    @Autowired
    private SingleTypeProcessor singleTypeProcessor;

    @Autowired
    @Qualifier("writer_Transaction_Type")
    private ClassifierCompositeItemWriter<TransactionTypeModel> classifierCompositeItemWriter;

    @Autowired
    @Qualifier("taskExecutor_Transaction_Type")
    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

    @Autowired
    private TransactionTypeClassifier transactionTypeClassifier;

    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - Export single type transactions
    @Bean
    public Step step_exportSingleType() {

        return new StepBuilder("exportSingleTypeStep", jobRepository)
                .<TransactionTypeModel, TransactionTypeModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(singleTypeProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        transactionTypeClassifier.closeAllwriters();
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - Export single type transactions
    @Bean
    public Job job_exportSingleType() {

        return new JobBuilder("exportSingleTypeJob", jobRepository)
                .start(step_exportSingleType())
                .build();
    }
}
