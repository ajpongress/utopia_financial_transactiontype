package com.capstone.transactiontype.Controllers;

import com.capstone.transactiontype.Services.TransactionTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TransactionTypeController {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    TransactionTypeService transactionTypeService;



    // ----------------------------------------------------------------------------------
    // --                               MAPPINGS                                       --
    // ----------------------------------------------------------------------------------

    // all transaction types
    @GetMapping("/types")
    public ResponseEntity<String> allTransactionTypesAPI(@RequestParam String source, @RequestParam String destination) {

        return transactionTypeService.exportAllTypes(source, destination);
    }

    // specific transaction type
    @GetMapping("/types/{typeID}")
    public ResponseEntity<String> oneTransactionTypeAPI(@PathVariable String typeID, @RequestParam String source, @RequestParam String destination) {

        return transactionTypeService.exportSingleType(typeID, source, destination);
    }
}
