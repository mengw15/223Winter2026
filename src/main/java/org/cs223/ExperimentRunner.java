package org.cs223;

import org.cs223.template.*;
import org.cs223.parser.InsertParser;
import java.io.File;
import java.util.*;

/**
 * Runs all experiment combinations automatically.
 * 2 workloads × 2 protocols × multiple thread counts × multiple contention levels
 */
public class ExperimentRunner {

    static final int[] THREAD_COUNTS = {1, 2, 4, 8};
    static final double[] CONTENTION_LEVELS = {0, 0.2, 0.5, 0.8, 1.0};
    static int[] HOTSET_SIZES = {5, 10, 20, 50, 100};
    static final int NUM_TRANSACTIONS = 10000;

    public static void main(String[] args) throws Exception {
        // Delete old summary file so we start fresh
        new File("results/summary.csv").delete();

        System.out.println("=== Starting Experiment Runner ===");
        System.out.println("Threads: " + Arrays.toString(THREAD_COUNTS));
        System.out.println("Contention levels: " + Arrays.toString(CONTENTION_LEVELS));
        System.out.println("Transactions per run: " + NUM_TRANSACTIONS);
        System.out.println("Hotset size: " + Arrays.toString(HOTSET_SIZES));
        System.out.println("Total runs: " + 2 * 2 * THREAD_COUNTS.length * CONTENTION_LEVELS.length);
        System.out.println();

        runAllWorkload1();
        runAllWorkload2();

        System.out.println("\n=== All experiments complete! Results in results/ ===");
    }

    static void runAllWorkload1() throws Exception {
        System.out.println("========== WORKLOAD 1: Bank Transfer ==========\n");

        for (TransactionManager.Protocol proto : TransactionManager.Protocol.values()) {
            for (int threads : THREAD_COUNTS) {
                for (double contention : CONTENTION_LEVELS) {
                    for (int hotsetSize : HOTSET_SIZES) {
                        System.out.printf("--- W1 | %s | threads=%d | contention=%.1f ---\n", proto, threads, contention);

                        // Fresh DB each run
                        String dbPath = "rundb_exp_w1";
                        deleteDirectory(new File(dbPath));
                        Database db = new Database(dbPath);

                        // Load data
                        List<String> allKeys = InsertParser.loadFromFile("Data/workload1/input1.txt", db);

                        List<TransactionTemplate> templates = List.of(new TransferTemplate());
                        List<List<List<String>>> keyPools = List.of(
                                List.of(allKeys, allKeys)
                        );

                        TransactionManager tm = new TransactionManager(db, proto);
                        tm.runWorkload(keyPools, hotsetSize, contention, threads, NUM_TRANSACTIONS, templates);

                        // Export CSV
                        Stats.appendSummary(1, proto.toString(), threads, contention, hotsetSize, NUM_TRANSACTIONS,
                                tm.getTotalCommitted(), tm.getTotalRetries(), tm.getLastRetryRate(),
                                tm.getLastThroughput(), tm.getAvgResponseTimeMs());
                        Stats.writeResponseTimes(1, proto.toString(), threads, contention,
                                tm.getResponseTimesByTemplate());

                        db.close();
                        System.out.println();
                    }
                }
            }
        }
    }

    static void runAllWorkload2() throws Exception {
        System.out.println("========== WORKLOAD 2: TPC-C Style ==========\n");

        for (TransactionManager.Protocol proto : TransactionManager.Protocol.values()) {
            for (int threads : THREAD_COUNTS) {
                for (double contention : CONTENTION_LEVELS) {
                    for(int hotsetSize : HOTSET_SIZES) {
                        System.out.printf("--- W2 | %s | threads=%d | contention=%.1f ---\n", proto, threads, contention);

                        String dbPath = "rundb_exp_w2";
                        deleteDirectory(new File(dbPath));
                        Database db = new Database(dbPath);

                        List<String> allKeys = InsertParser.loadFromFile("Data/workload2/input2.txt", db);
                        List<String> warehouseKeys = InsertParser.filterKeysByPrefix(allKeys, "W_");
                        List<String> districtKeys = InsertParser.filterKeysByPrefix(allKeys, "D_");
                        List<String> customerKeys = InsertParser.filterKeysByPrefix(allKeys, "C_");
                        List<String> stockKeys = InsertParser.filterKeysByPrefix(allKeys, "S_");

                        List<TransactionTemplate> templates = List.of(new NewOrderTemplate(), new PaymentTemplate());
                        List<List<List<String>>> keyPools = List.of(
                                List.of(districtKeys, stockKeys, stockKeys, stockKeys),
                                List.of(warehouseKeys, districtKeys, customerKeys)
                        );

                        TransactionManager tm = new TransactionManager(db, proto);
                        tm.runWorkload(keyPools, hotsetSize, contention, threads, NUM_TRANSACTIONS, templates);

                        Stats.appendSummary(2, proto.toString(), threads, contention, hotsetSize, NUM_TRANSACTIONS,
                                tm.getTotalCommitted(), tm.getTotalRetries(), tm.getLastRetryRate(),
                                tm.getLastThroughput(), tm.getAvgResponseTimeMs());
                        Stats.writeResponseTimes(2, proto.toString(), threads, contention,
                                tm.getResponseTimesByTemplate());

                        db.close();
                        System.out.println();
                    }
                }
            }
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
}
