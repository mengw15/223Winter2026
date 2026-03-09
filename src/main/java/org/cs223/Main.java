package org.cs223;

import org.cs223.template.*;
import org.cs223.parser.InsertParser;
import java.io.File;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        // Defaults
        int workloadNum = 1;
        String protocol = "occ";
        int threads = 4;
        double contention = 0.5;
        int hotsetSize = 10;
        int numTransactions = 1000;

        // Parse args
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--workload" -> workloadNum = Integer.parseInt(args[++i]);
                case "--protocol" -> protocol = args[++i].toLowerCase();
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--contention" -> contention = Double.parseDouble(args[++i]);
                case "--hotset" -> hotsetSize = Integer.parseInt(args[++i]);
                case "--transactions" -> numTransactions = Integer.parseInt(args[++i]);
            }
        }

        TransactionManager.Protocol proto = protocol.equals("2pl")
                ? TransactionManager.Protocol.TWO_PL
                : TransactionManager.Protocol.OCC;

        System.out.println("Workload: " + workloadNum);
        System.out.println("Protocol: " + proto);
        System.out.println("Threads: " + threads);
        System.out.println("Contention: " + contention);
        System.out.println("Hotset: " + hotsetSize);
        System.out.println("Transactions: " + numTransactions);
        System.out.println();

        try {
            // Delete old DB and create fresh one
            String dbPath = "rundb_w" + workloadNum + "_" + protocol;
            deleteDirectory(new File(dbPath));
            Database db = new Database(dbPath);

            if (workloadNum == 1) {
                runWorkload1(db, proto, threads, contention, hotsetSize, numTransactions);
            } else {
                runWorkload2(db, proto, threads, contention, hotsetSize, numTransactions);
            }

            db.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) deleteDirectory(f);
            }
            dir.delete();
        }
    }

    static void runWorkload1(Database db, TransactionManager.Protocol proto,
                             int threads, double contention, int hotsetSize, int numTxns) throws Exception {
        // Load data
        List<String> allKeys = InsertParser.loadFromFile("Data/workload1/input1.txt", db);
        System.out.println("Loaded " + allKeys.size() + " keys");

        // All keys are accounts (A_*), both FROM and TO pick from same pool
        List<TransactionTemplate> templates = List.of(new TransferTemplate());
        List<List<List<String>>> keyPools = List.of(
                List.of(allKeys, allKeys)  // Transfer: [FROM_KEY pool, TO_KEY pool]
        );

        TransactionManager tm = new TransactionManager(db, proto);
        tm.runWorkload(keyPools, hotsetSize, contention, threads, numTxns, templates);
        exportStats(1, proto, threads, contention, hotsetSize, numTxns, tm);
    }

    static void runWorkload2(Database db, TransactionManager.Protocol proto,
                             int threads, double contention, int hotsetSize, int numTxns) throws Exception {
        // Load data
        List<String> allKeys = InsertParser.loadFromFile("Data/workload2/input2.txt", db);
        System.out.println("Loaded " + allKeys.size() + " keys");

        // Separate keys by type
        List<String> warehouseKeys = InsertParser.filterKeysByPrefix(allKeys, "W_");
        List<String> districtKeys = InsertParser.filterKeysByPrefix(allKeys, "D_");
        List<String> customerKeys = InsertParser.filterKeysByPrefix(allKeys, "C_");
        List<String> stockKeys = InsertParser.filterKeysByPrefix(allKeys, "S_");

        System.out.println("Warehouses: " + warehouseKeys.size());
        System.out.println("Districts: " + districtKeys.size());
        System.out.println("Customers: " + customerKeys.size());
        System.out.println("Stocks: " + stockKeys.size());

        List<TransactionTemplate> templates = List.of(new NewOrderTemplate(), new PaymentTemplate());
        List<List<List<String>>> keyPools = List.of(
                List.of(districtKeys, stockKeys, stockKeys, stockKeys),   // NewOrder: D_KEY, S_KEY_1, S_KEY_2, S_KEY_3
                List.of(warehouseKeys, districtKeys, customerKeys)        // Payment: W_KEY, D_KEY, C_KEY
        );

        TransactionManager tm = new TransactionManager(db, proto);
        tm.runWorkload(keyPools, hotsetSize, contention, threads, numTxns, templates);
        exportStats(2, proto, threads, contention, hotsetSize, numTxns, tm);
    }

    static void exportStats(int workload, TransactionManager.Protocol proto,
                            int threads, double contention, int hotsetSize, int numTxns,
                            TransactionManager tm) throws Exception {
        Stats.appendSummary(workload, proto.toString(), threads, contention, hotsetSize, numTxns,
                tm.getTotalCommitted(), tm.getTotalRetries(), tm.getLastRetryRate(),
                tm.getLastThroughput(), tm.getAvgResponseTimeMs());

        Stats.writeResponseTimes(workload, proto.toString(), threads, contention, hotsetSize,
                tm.getResponseTimesByTemplate());

        System.out.println("\nResults exported to results/");
    }
}
