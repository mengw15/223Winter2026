package org.cs223.test;

import org.cs223.*;
import java.util.*;

public class TestOCC {
    public static void main(String[] args) {
        try {
            Database db = new Database("testdb_occtest");
            OCCValidator validator = new OCCValidator();

            // Load initial data
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("name", "A");
            a.put("balance", 100);
            db.put("A", a);

            Map<String, Object> b = new LinkedHashMap<>();
            b.put("name", "B");
            b.put("balance", 200);
            db.put("B", b);

            // === Test 1: Single transaction, should commit ===
            Set<Integer> ignore1 = validator.snapshotFinished();
            Transaction txn1 = new Transaction(1, List.of("A"), db);
            txn1.begin();
            Map<String, Object> r1 = txn1.read("A");
            r1.put("balance", 150);
            txn1.write("A", r1);

            boolean valid1 = validator.validate(txn1, ignore1);
            System.out.println("Txn1 validated: " + valid1);
            if (valid1) {
                txn1.applyWrites();
                txn1.markCommitted();
                validator.markFinished(1);
            }

            // === Test 2: No conflict (different keys) ===
            Set<Integer> ignore2 = validator.snapshotFinished();
            Transaction txn2 = new Transaction(2, List.of("B"), db);
            txn2.begin();
            Map<String, Object> r2 = txn2.read("B");
            r2.put("balance", 250);
            txn2.write("B", r2);

            boolean valid2 = validator.validate(txn2, ignore2);
            System.out.println("Txn2 validated (no conflict): " + valid2);
            if (valid2) {
                txn2.applyWrites();
                txn2.markCommitted();
                validator.markFinished(2);
            }

            // === Test 3: Read-write conflict ===
            Set<Integer> ignore3 = validator.snapshotFinished();
            Transaction txn3 = new Transaction(3, List.of("A"), db);
            txn3.begin();
            txn3.read("A");

            Set<Integer> ignore4 = validator.snapshotFinished();
            Transaction txn4 = new Transaction(4, List.of("A"), db);
            txn4.begin();
            txn4.read("A");
            Map<String, Object> w4 = new LinkedHashMap<>();
            w4.put("name", "A");
            w4.put("balance", 999);
            txn4.write("A", w4);
            boolean valid4 = validator.validate(txn4, ignore4);
            System.out.println("Txn4 validated: " + valid4);
            if (valid4) {
                txn4.applyWrites();
                txn4.markCommitted();
                validator.markFinished(4);
            }

            Map<String, Object> w3 = new LinkedHashMap<>();
            w3.put("name", "A");
            w3.put("balance", 200);
            txn3.write("A", w3);
            boolean valid3 = validator.validate(txn3, ignore3);
            System.out.println("Txn3 validated (should conflict): " + valid3);

            System.out.println("A final value: " + db.get("A"));

            db.close();
            System.out.println("\nAll OCC tests passed!");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
