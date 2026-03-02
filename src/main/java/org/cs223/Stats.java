package org.cs223;

import java.io.*;
import java.util.List;
import java.util.Map;

public class Stats {

    private static final String RESULTS_DIR = "results";
    private static final String SUMMARY_FILE = RESULTS_DIR + "/summary.csv";

    /**
     * Ensure results directory exists.
     */
    private static void ensureDir() {
        new File(RESULTS_DIR).mkdirs();
    }

    /**
     * Append one row to summary.csv (creates file with header if it doesn't exist).
     */
    public static void appendSummary(int workload, String protocol, int threads,
                                      double contention, int hotset, int transactions,
                                      int committed, int retries, double retryRate,
                                      double throughput, double avgResponseTime) throws IOException {
        ensureDir();
        boolean fileExists = new File(SUMMARY_FILE).exists();

        try (PrintWriter pw = new PrintWriter(new FileWriter(SUMMARY_FILE, true))) {
            if (!fileExists) {
                pw.println("workload,protocol,threads,contention,hotset,transactions,committed,retries,retry_rate,throughput,avg_response_time");
            }
            pw.printf("%d,%s,%d,%.2f,%d,%d,%d,%d,%.2f,%.2f,%.4f%n",
                    workload, protocol, threads, contention, hotset, transactions,
                    committed, retries, retryRate, throughput, avgResponseTime);
        }
    }

    /**
     * Write per-transaction response times to a CSV file.
     */
    public static void writeResponseTimes(int workload, String protocol, int threads,
                                           double contention,
                                           Map<String, List<Double>> responseTimesByTemplate) throws IOException {
        ensureDir();
        String filename = String.format("%s/rt_w%d_%s_t%d_c%.2f.csv",
                RESULTS_DIR, workload, protocol, threads, contention);

        try (PrintWriter pw = new PrintWriter(new FileWriter(filename))) {
            pw.println("template,response_time_ms");
            for (Map.Entry<String, List<Double>> entry : responseTimesByTemplate.entrySet()) {
                String template = entry.getKey();
                for (Double rt : entry.getValue()) {
                    pw.printf("%s,%.4f%n", template, rt);
                }
            }
        }
    }
}
