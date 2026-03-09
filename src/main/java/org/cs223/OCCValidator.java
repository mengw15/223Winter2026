package org.cs223;

import java.util.*;

public class OCCValidator {

    // Transactions that passed validation (may still be in write phase)
    private final List<ValidatedRecord> validated = new ArrayList<>();
    // Set of txnIds that have finished their write phase
    private final Set<Integer> finished = new HashSet<>();

    private static class ValidatedRecord {
        final int txnId;
        final Set<String> writeSet;

        ValidatedRecord(int txnId, Set<String> writeSet) {
            this.txnId = txnId;
            this.writeSet = writeSet;
        }
    }

    /**
     * Take a snapshot of finished transactions at transaction start.
     * These will be ignored during validation since they completed before we started.
     */
    public synchronized Set<Integer> snapshotFinished() {
        return new HashSet<>(finished);
    }

    /**
     * Validate a transaction.
     * - Check 1: For all Ti validated after we started: RS(Tj) ∩ WS(Ti) = empty
     * - Check 2: For all Ti validated after we started AND not yet finished: WS(Tj) ∩ WS(Ti) = empty
     * If valid, adds to validated set and returns true. Write phase happens OUTSIDE this method.
     */
    public synchronized boolean validate(Transaction txn, Set<Integer> ignoreTxns) {
        Set<String> readKeys = txn.getReadSet().keySet();
        Set<String> writeKeys = txn.getWriteBuffer().keySet();

        for (ValidatedRecord record : validated) {
            // Skip transactions that finished before we started
            if (ignoreTxns.contains(record.txnId)) {
                continue;
            }

            // Check 1: no one who validated after we started wrote what we read
            for (String writeKey : record.writeSet) {
                if (readKeys.contains(writeKey)) {
                    return false;
                }
            }

            // Check 2: if Ti hasn't finished writing yet, our write set can't overlap
            if (!finished.contains(record.txnId)) {
                for (String writeKey : record.writeSet) {
                    if (writeKeys.contains(writeKey)) {
                        return false;
                    }
                }
            }
        }

        // Validation passed: add to validated set
        validated.add(new ValidatedRecord(txn.getTxnId(), new HashSet<>(writeKeys)));
        return true;
    }

    /**
     * Called after write phase completes. Marks the transaction as finished.
     */
    public synchronized void markFinished(int txnId) {
        finished.add(txnId);
    }

    /**
     * Clear all state (call between workload runs).
     */
    public synchronized void reset() {
        validated.clear();
        finished.clear();
    }
}
