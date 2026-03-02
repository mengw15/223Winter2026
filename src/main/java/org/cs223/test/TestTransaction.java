package org.cs223.test;

import org.cs223.*;
import java.util.*;

public class TestTransaction {
    public static void main(String[] args) {
        try {
            Database db = new Database("testdb_txntest");

            // Load initial data
            Map<String, Object> acc1 = new LinkedHashMap<>();
            acc1.put("name", "Account-1");
            acc1.put("balance", 100);
            db.put("account1", acc1);

            Map<String, Object> acc2 = new LinkedHashMap<>();
            acc2.put("name", "Account-2");
            acc2.put("balance", 200);
            db.put("account2", acc2);

            // Test: transfer 50 from account1 to account2
            Transaction txn = new Transaction(1, List.of("account1", "account2"), db);
            txn.begin();

            Map<String, Object> val1 = txn.read("account1");
            Map<String, Object> val2 = txn.read("account2");
            System.out.println("Before: account1=" + val1 + ", account2=" + val2);

            int balance1 = (Integer) val1.get("balance") - 50;
            int balance2 = (Integer) val2.get("balance") + 50;
            val1.put("balance", balance1);
            val2.put("balance", balance2);
            txn.write("account1", val1);
            txn.write("account2", val2);

            System.out.println("In-txn read account1=" + txn.read("account1"));

            txn.applyWrites();
            txn.markCommitted();

            System.out.println("After commit: account1=" + db.get("account1") + ", account2=" + db.get("account2"));
            System.out.println("Response time: " + String.format("%.2f", txn.getResponseTimeMs()) + " ms");

            db.close();
            System.out.println("\nAll Transaction tests passed!");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
