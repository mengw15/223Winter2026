package org.cs223.template;

import org.cs223.Transaction;
import java.util.List;
import java.util.Map;

/**
 * Workload 2: Payment transaction (TPC-C style).
 * Reads warehouse, district, customer; updates ytd/balance/payment_cnt.
 * INPUTS: W_KEY, D_KEY, C_KEY
 */
public class PaymentTemplate implements TransactionTemplate {
    public int getNumKeys() { return 3; }
    public String getName() { return "Payment"; }

    public void execute(Transaction txn, List<String> keys) throws Exception {
        String wKey = keys.get(0);
        String dKey = keys.get(1);
        String cKey = keys.get(2);

        // Update warehouse ytd
        Map<String, Object> w = txn.read(wKey);
        w.put("ytd", (Integer) w.get("ytd") + 5);
        txn.write(wKey, w);

        // Update district ytd
        Map<String, Object> d = txn.read(dKey);
        d.put("ytd", (Integer) d.get("ytd") + 5);
        txn.write(dKey, d);

        // Update customer
        Map<String, Object> c = txn.read(cKey);
        c.put("balance", (Integer) c.get("balance") - 5);
        c.put("ytd_payment", (Integer) c.get("ytd_payment") + 5);
        c.put("payment_cnt", (Integer) c.get("payment_cnt") + 1);
        txn.write(cKey, c);
    }
}
