package org.cs223.template;

import org.cs223.Transaction;
import java.util.List;
import java.util.Map;

/**
 * Workload 2: New Order transaction (TPC-C style).
 * Reads 1 district + 3 stocks, updates them.
 * INPUTS: D_KEY, S_KEY_1, S_KEY_2, S_KEY_3
 */
public class NewOrderTemplate implements TransactionTemplate {
    public int getNumKeys() { return 4; }
    public String getName() { return "NewOrder"; }

    public void execute(Transaction txn, List<String> keys) throws Exception {
        String dKey = keys.get(0);
        String sKey1 = keys.get(1);
        String sKey2 = keys.get(2);
        String sKey3 = keys.get(3);

        // Read district and increment next_o_id
        Map<String, Object> d = txn.read(dKey);
        int oId = (Integer) d.get("next_o_id");
        d.put("next_o_id", oId + 1);
        txn.write(dKey, d);

        // Update stock 1
        Map<String, Object> s1 = txn.read(sKey1);
        s1.put("qty", (Integer) s1.get("qty") - 1);
        s1.put("ytd", (Integer) s1.get("ytd") + 1);
        s1.put("order_cnt", (Integer) s1.get("order_cnt") + 1);
        txn.write(sKey1, s1);

        // Update stock 2
        Map<String, Object> s2 = txn.read(sKey2);
        s2.put("qty", (Integer) s2.get("qty") - 1);
        s2.put("ytd", (Integer) s2.get("ytd") + 1);
        s2.put("order_cnt", (Integer) s2.get("order_cnt") + 1);
        txn.write(sKey2, s2);

        // Update stock 3
        Map<String, Object> s3 = txn.read(sKey3);
        s3.put("qty", (Integer) s3.get("qty") - 1);
        s3.put("ytd", (Integer) s3.get("ytd") + 1);
        s3.put("order_cnt", (Integer) s3.get("order_cnt") + 1);
        txn.write(sKey3, s3);
    }
}
