package org.cs223;

import java.util.*;

public class Transaction {
    private final int txnId;
    private final List<String> inputKeys;
    private final Map<String, Map<String, Object>> readSet;
    private final Map<String, Map<String, Object>> writeBuffer;
    private final Database db;

    private long startTime;
    private long endTime;

    public Transaction(int txnId, List<String> inputKeys, Database db) {
        this.txnId = txnId;
        this.inputKeys = inputKeys;
        this.readSet = new HashMap<>();
        this.writeBuffer = new HashMap<>();
        this.db = db;
    }

    public void begin() {
        startTime = System.nanoTime();
        readSet.clear();
        writeBuffer.clear();
    }

    public Map<String, Object> read(String key) throws Exception {
        // Check private write buffer first
        if (writeBuffer.containsKey(key)) {
            Map<String, Object> value = new LinkedHashMap<>(writeBuffer.get(key));
            readSet.put(key, value);
            return value;
        }
        // Read from DB
        Map<String, Object> value = db.get(key);
        if (value != null) {
            value = new LinkedHashMap<>(value); // defensive copy
        }
        readSet.put(key, value);
        return value;
    }

    public void write(String key, Map<String, Object> value) {
        writeBuffer.put(key, new LinkedHashMap<>(value));
    }

    public void applyWrites() throws Exception {
        for (Map.Entry<String, Map<String, Object>> entry : writeBuffer.entrySet()) {
            db.put(entry.getKey(), entry.getValue());
        }
    }

    public void markCommitted() {
        endTime = System.nanoTime();
    }

    public int getTxnId() { return txnId; }
    public List<String> getInputKeys() { return inputKeys; }
    public Map<String, Map<String, Object>> getReadSet() { return readSet; }
    public Map<String, Map<String, Object>> getWriteBuffer() { return writeBuffer; }

    public double getResponseTimeMs() {
        return (endTime - startTime) / 1_000_000.0;
    }
}
