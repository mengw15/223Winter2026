package org.cs223.template;

import org.cs223.Transaction;
import java.util.List;
import java.util.Map;

/**
 * Workload 1: Transfer transaction.
 * Reads two accounts, transfers 1 from FROM to TO.
 * INPUTS: FROM_KEY, TO_KEY
 */
public class TransferTemplate implements TransactionTemplate {
    public int getNumKeys() { return 2; }
    public String getName() { return "Transfer"; }

    public void execute(Transaction txn, List<String> keys) throws Exception {
        String fromKey = keys.get(0);
        String toKey = keys.get(1);

        Map<String, Object> fromAcc = txn.read(fromKey);
        Map<String, Object> toAcc = txn.read(toKey);

        int fromBalance = (Integer) fromAcc.get("balance");
        int toBalance = (Integer) toAcc.get("balance");

        fromAcc.put("balance", fromBalance - 1);
        toAcc.put("balance", toBalance + 1);

        txn.write(fromKey, fromAcc);
        txn.write(toKey, toAcc);
    }
}
