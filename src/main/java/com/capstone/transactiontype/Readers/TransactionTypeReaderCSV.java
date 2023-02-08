package com.capstone.transactiontype.Readers;

import com.capstone.transactiontype.Models.TransactionTypeModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TransactionTypeReaderCSV {

    // FlatFileItemReader
    @StepScope
    @Bean("reader_Transaction_Type")
    public SynchronizedItemStreamReader<TransactionTypeModel> synchronizedItemStreamReader(
            @Value("#{jobParameters['file.input']}")
            String source_input
    ) throws UnexpectedInputException,
            NonTransientResourceException, ParseException {

        FlatFileItemReader<TransactionTypeModel> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(source_input));
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper((line, lineNumber) -> {
            String[] fields = line.split(",");
            TransactionTypeModel transaction = new TransactionTypeModel();

            transaction.setUserID(Long.parseLong(fields[0]));
            transaction.setCardID(Long.parseLong(fields[1]));
            transaction.setTransactionYear(fields[2]);
            transaction.setTransactionMonth(fields[3]);
            transaction.setTransactionDay(fields[4]);
            transaction.setTransactionTime(fields[5]);
            transaction.setTransactionAmount(fields[6]);
            transaction.setTransactionType(fields[7]);
            transaction.setMerchantID(Long.parseLong(fields[8]));
            transaction.setTransactionCity(fields[9]);
            transaction.setTransactionState(fields[10]);
            transaction.setTransactionZip(fields[11]);
            transaction.setMerchantCatCode(Long.parseLong(fields[12]));
            transaction.setTransactionErrorCheck(fields[13]);
            transaction.setTransactionFraudCheck(fields[14]);

            return transaction;
        });

        // Make FlatFileItemReader thread-safe
        return new SynchronizedItemStreamReaderBuilder<TransactionTypeModel>()
                .delegate(itemReader)
                .build();
    }
}
