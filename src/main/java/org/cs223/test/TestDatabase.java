package org.cs223.test;

import org.cs223.Database;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestDatabase {
    public static void main(String[] args) {
        try {
            Database db = new Database("testdb_dbtest");

            // Insert
            Map<String, Object> val1 = new LinkedHashMap<>();
            val1.put("name", "Account-1");
            val1.put("balance", 100);
            db.put("key1", val1);

            Map<String, Object> val2 = new LinkedHashMap<>();
            val2.put("name", "Account-2");
            val2.put("balance", 200);
            db.put("key2", val2);

            // Read
            System.out.println("key1 = " + db.get("key1"));
            System.out.println("key2 = " + db.get("key2"));
            System.out.println("key3 = " + db.get("key3"));

            // Update
            val1.put("balance", 150);
            db.put("key1", val1);
            System.out.println("key1 after update = " + db.get("key1"));

            // Delete
            db.delete("key2");
            System.out.println("key2 after delete = " + db.get("key2"));

            db.close();
            System.out.println("\nAll Database tests passed!");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
