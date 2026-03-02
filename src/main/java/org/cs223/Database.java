package org.cs223;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;

public class Database {
    private RocksDB db;
    private Options options;

    public Database(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath);
    }

    public Map<String, Object> get(String key) throws RocksDBException {
        byte[] value = db.get(key.getBytes());
        if (value == null) return null;
        return deserialize(new String(value));
    }

    public void put(String key, Map<String, Object> value) throws RocksDBException {
        db.put(key.getBytes(), serialize(value).getBytes());
    }

    public void delete(String key) throws RocksDBException {
        db.delete(key.getBytes());
    }

    public void close() {
        if (db != null) db.close();
        if (options != null) options.close();
    }

    /**
     * Serialize a map to string format: {key1: value1, key2: value2, ...}
     */
    public static String serialize(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(", ");
            sb.append(entry.getKey()).append(": ");
            if (entry.getValue() instanceof String) {
                sb.append("\"").append(entry.getValue()).append("\"");
            } else {
                sb.append(entry.getValue());
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Deserialize string format {key1: value1, key2: "str", ...} to map.
     */
    public static Map<String, Object> deserialize(String s) {
        Map<String, Object> map = new LinkedHashMap<>();
        s = s.trim();
        if (s.startsWith("{")) s = s.substring(1);
        if (s.endsWith("}")) s = s.substring(0, s.length() - 1);

        // Split by comma, but not commas inside quotes
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (char c : s.toCharArray()) {
            if (c == '"') inQuotes = !inQuotes;
            if (c == ',' && !inQuotes) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) parts.add(current.toString());

        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) continue;
            int colonIdx = part.indexOf(':');
            if (colonIdx < 0) continue;
            String key = part.substring(0, colonIdx).trim();
            String val = part.substring(colonIdx + 1).trim();

            if (val.startsWith("\"") && val.endsWith("\"")) {
                map.put(key, val.substring(1, val.length() - 1));
            } else {
                try {
                    map.put(key, Integer.parseInt(val));
                } catch (NumberFormatException e) {
                    map.put(key, val);
                }
            }
        }
        return map;
    }
}
