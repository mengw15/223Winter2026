package org.cs223.template;

import org.cs223.Transaction;
import java.util.List;

/**
 * Defines what a transaction does.
 * Implementations specify the read/write logic.
 */
public interface TransactionTemplate {
    int getNumKeys();
    void execute(Transaction txn, List<String> keys) throws Exception;
    String getName();
}
