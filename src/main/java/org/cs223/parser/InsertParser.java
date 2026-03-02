package org.cs223.parser;

import org.cs223.Database;
import java.io.*;
import java.util.*;

public class InsertParser {

    /**
     * Parse an INSERT file and load data into the database.
     * Returns the list of all keys loaded.
     */
    public static List<String> loadFromFile(String filePath, Database db) throws Exception {
        List<String> keys = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.equals("INSERT") || line.equals("END")) {
                    continue;
                }
                if (line.startsWith("KEY:")) {
                    // Parse: KEY: <key>, VALUE: {<map>}
                    int valueIdx = line.indexOf("VALUE:");
                    if (valueIdx < 0) continue;

                    String key = line.substring(4, line.indexOf(",", 4)).trim();
                    String valueStr = line.substring(valueIdx + 6).trim();

                    Map<String, Object> value = Database.deserialize(valueStr);
                    db.put(key, value);
                    keys.add(key);
                }
            }
        }
        return keys;
    }

    /**
     * Get keys filtered by prefix (e.g., "W_" for warehouses, "D_" for districts).
     */
    public static List<String> filterKeysByPrefix(List<String> keys, String prefix) {
        List<String> filtered = new ArrayList<>();
        for (String key : keys) {
            if (key.startsWith(prefix)) {
                filtered.add(key);
            }
        }
        return filtered;
    }
}
