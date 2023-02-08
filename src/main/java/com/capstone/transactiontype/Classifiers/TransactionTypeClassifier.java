package com.capstone.transactiontype.Classifiers;

import com.capstone.transactiontype.Models.TransactionTypeModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.classify.Classifier;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@StepScope
@Component
@Slf4j
public class TransactionTypeClassifier implements Classifier<TransactionTypeModel, ItemWriter<? super TransactionTypeModel>> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // Destination path for export file
    @Value("#{jobParameters['outputPath_param']}")
    private String outputPath;

    // Map for mapping each typeID to its own dedicated ItemWriter (for performance)
    private final Map<String, ItemWriter<? super TransactionTypeModel>> writerMap;

    // Public constructor
    public TransactionTypeClassifier() {
        this.writerMap = new HashMap<>();
    }



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    // Classify method (contains XML writer and synchronized item stream writer)
    @Override
    public ItemWriter<? super TransactionTypeModel> classify(TransactionTypeModel transaction) {

        // Set filename to specific typeID from the Transaction model
        String fileName = transaction.getFileName();

        // Make entire process thead-safe
        synchronized (this) {

            // If typeID has already been accessed, use the same ItemWriter
            if (writerMap.containsKey(fileName)) {
                return writerMap.get(fileName);
            }
            // Create new ItemWriter for new TypeID
            else {

                // Complete path for file export
                File file = new File(outputPath + "\\" + fileName);

                // XML writer
                XStreamMarshaller marshaller = new XStreamMarshaller();
                marshaller.setAliases(Collections.singletonMap("transaction", TransactionTypeModel.class));

                StaxEventItemWriter<TransactionTypeModel> writerXML = new StaxEventItemWriterBuilder<TransactionTypeModel>()
                        .name("typeXmlWriter")
                        .resource(new FileSystemResource(file))
                        .marshaller(marshaller)
                        .rootTagName("transactions")
                        .transactional(false) // Keeps XML headers on all output files
                        .build();

                // Make XML writer thread-safe
                SynchronizedItemStreamWriter<TransactionTypeModel> synchronizedItemStreamWriter =
                        new SynchronizedItemStreamWriterBuilder<TransactionTypeModel>()
                                .delegate(writerXML)
                                .build();

                writerXML.open(new ExecutionContext());
                writerMap.put(fileName, synchronizedItemStreamWriter); // Pair TypeID to unique ItemWriter
                return synchronizedItemStreamWriter;
            }
        }
    }

    public void closeAllwriters() {

        for (String key : writerMap.keySet()) {

            SynchronizedItemStreamWriter<TransactionTypeModel> writer = (SynchronizedItemStreamWriter<TransactionTypeModel>) writerMap.get(key);
            writer.close();
        }
        writerMap.clear();
    }


}
