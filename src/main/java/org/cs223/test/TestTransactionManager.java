package org.cs223.test;

import org.cs223.*;
import org.cs223.template.*;
import java.util.*;

public class TestTransactionManager {
    public static void main(String[] args) {
        try {
            Database db = new Database("testdb_tmtest");

            // Load 100 accounts
            List<String> allKeys = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String key = "A_" + i;
                Map<String, Object> val = new LinkedHashMap<>();
                val.put("name", "Account-" + i);
                val.put("balance", 1000);
                db.put(key, val);
                allKeys.add(key);
            }

            List<TransactionTemplate> templates = List.of(new TransferTemplate());
            List<List<List<String>>> keyPools = List.of(
                    List.of(allKeys, allKeys)
            );

            // Test 2PL
            System.out.println("--- Conservative 2PL ---");
            TransactionManager tm2pl = new TransactionManager(db, TransactionManager.Protocol.TWO_PL);
            tm2pl.runWorkload(keyPools, 10, 0.5, 4, 200, templates);

            // Reset data
            for (String key : allKeys) {
                Map<String, Object> val = new LinkedHashMap<>();
                val.put("name", "Account-" + key.substring(2));
                val.put("balance", 1000);
                db.put(key, val);
            }

            // Test OCC
            System.out.println("\n--- OCC ---");
            TransactionManager tmOcc = new TransactionManager(db, TransactionManager.Protocol.OCC);
            tmOcc.runWorkload(keyPools, 10, 0.5, 4, 200, templates);

            db.close();
            System.out.println("\nAll TransactionManager tests passed!");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
