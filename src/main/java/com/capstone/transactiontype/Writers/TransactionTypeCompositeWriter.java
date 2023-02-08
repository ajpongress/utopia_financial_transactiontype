package com.capstone.transactiontype.Writers;

import com.capstone.transactiontype.Classifiers.TransactionTypeClassifier;
import com.capstone.transactiontype.Models.TransactionTypeModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TransactionTypeCompositeWriter {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    TransactionTypeClassifier transactionTypeClassifier;



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    @Bean("writer_Transaction_Type")
    public ClassifierCompositeItemWriter<TransactionTypeModel> classifierCompositeItemWriter() {

        ClassifierCompositeItemWriter<TransactionTypeModel> writer = new ClassifierCompositeItemWriter<>();
        writer.setClassifier(transactionTypeClassifier);

        return writer;
    }
}
