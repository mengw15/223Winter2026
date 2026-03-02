package org.cs223;

import org.cs223.template.TransactionTemplate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionManager {

    public enum Protocol { OCC, TWO_PL }

    private final Database db;
    private final Protocol protocol;
    private final LockManager lockManager;
    private final OCCValidator occValidator;

    // Stats
    private final List<Double> responseTimes = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger totalCommitted = new AtomicInteger(0);
    private final AtomicInteger totalRetries = new AtomicInteger(0);
    private final AtomicInteger txnIdCounter = new AtomicInteger(0);
    // Per-template stats
    private final Map<String, AtomicInteger> commitsByTemplate = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> retriesByTemplate = new ConcurrentHashMap<>();
    private final Map<String, List<Double>> responseTimesByTemplate = new ConcurrentHashMap<>();

    public TransactionManager(Database db, Protocol protocol) {
        this.db = db;
        this.protocol = protocol;
        this.lockManager = new LockManager();
        this.occValidator = new OCCValidator();
    }

    /**
     * Run a workload with key pools per template input slot.
     *
     * @param keyPools  For each template, a list of key pools (one per input slot).
     *                  e.g., NewOrder needs [districtKeys, stockKeys, stockKeys, stockKeys]
     * @param hotsetSize     Number of hot keys per pool
     * @param contention     Probability of picking from hotset
     * @param numThreads     Number of worker threads
     * @param numTransactions Total transactions to execute
     * @param templates      Transaction templates
     */
    public void runWorkload(List<List<List<String>>> keyPools, int hotsetSize, double contention,
                            int numThreads, int numTransactions,
                            List<TransactionTemplate> templates) {

        // Reset stats
        responseTimes.clear();
        totalCommitted.set(0);
        totalRetries.set(0);
        txnIdCounter.set(0);
        occValidator.reset();
        commitsByTemplate.clear();
        retriesByTemplate.clear();
        responseTimesByTemplate.clear();

        for (TransactionTemplate t : templates) {
            commitsByTemplate.put(t.getName(), new AtomicInteger(0));
            retriesByTemplate.put(t.getName(), new AtomicInteger(0));
            responseTimesByTemplate.put(t.getName(), Collections.synchronizedList(new ArrayList<>()));
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long startTime = System.nanoTime();

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numTransactions; i++) {
            final int templateIdx = i % templates.size();
            final TransactionTemplate template = templates.get(templateIdx);
            final List<List<String>> pools = keyPools.get(templateIdx);
            futures.add(executor.submit(() -> {
                executeTransaction(template, pools, hotsetSize, contention);
            }));
        }

        for (Future<?> f : futures) {
            try { f.get(); } catch (Exception e) { e.printStackTrace(); }
        }

        long endTime = System.nanoTime();
        double totalTimeSec = (endTime - startTime) / 1_000_000_000.0;

        executor.shutdown();

        // Print results
        System.out.println("=== Results (" + protocol + ") ===");
        System.out.println("Threads: " + numThreads);
        System.out.println("Contention: " + contention);
        System.out.println("Hotset size: " + hotsetSize);
        System.out.println("Committed: " + totalCommitted.get());
        System.out.println("Total retries: " + totalRetries.get());
        int totalAttempts = totalCommitted.get() + totalRetries.get();
        System.out.printf("Retry rate: %.2f%%\n", totalAttempts > 0 ? totalRetries.get() * 100.0 / totalAttempts : 0);
        System.out.printf("Throughput: %.2f txns/sec\n", totalCommitted.get() / totalTimeSec);
        System.out.printf("Avg response time: %.4f ms\n", getAvgResponseTimeMs());

        // Per-template stats
        for (TransactionTemplate t : templates) {
            String name = t.getName();
            int commits = commitsByTemplate.get(name).get();
            int retries = retriesByTemplate.get(name).get();
            List<Double> times = responseTimesByTemplate.get(name);
            double avgTime = times.isEmpty() ? 0 : times.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            System.out.printf("  [%s] Committed: %d, Retries: %d, Avg RT: %.4f ms\n", name, commits, retries, avgTime);
        }
    }

    private void executeTransaction(TransactionTemplate template, List<List<String>> pools,
                                     int hotsetSize, double contention) {
        Random rand = ThreadLocalRandom.current();
        int maxRetries = 100;
        String templateName = template.getName();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            // Select keys: one from each pool
            List<String> selectedKeys = new ArrayList<>();
            Set<String> usedKeys = new HashSet<>();
            for (List<String> pool : pools) {
                String key;
                do {
                    key = selectOneKey(pool, hotsetSize, contention, rand);
                } while (usedKeys.contains(key));
                usedKeys.add(key);
                selectedKeys.add(key);
            }

            int txnId = txnIdCounter.incrementAndGet();

            boolean success;
            if (protocol == Protocol.TWO_PL) {
                success = executeTwoPL(txnId, selectedKeys, template);
            } else {
                success = executeOCC(txnId, selectedKeys, template);
            }

            if (success) {
                totalCommitted.incrementAndGet();
                commitsByTemplate.get(templateName).incrementAndGet();
                return;
            }

            // Retry with exponential backoff + jitter
            totalRetries.incrementAndGet();
            retriesByTemplate.get(templateName).incrementAndGet();
            try {
                long backoff = (long) (Math.pow(2, Math.min(attempt, 10)) + rand.nextInt(5));
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private String selectOneKey(List<String> pool, int hotsetSize, double contention, Random rand) {
        int effectiveHotset = Math.min(hotsetSize, pool.size());
        if (rand.nextDouble() < contention && effectiveHotset > 0) {
            return pool.get(rand.nextInt(effectiveHotset));
        } else {
            return pool.get(rand.nextInt(pool.size()));
        }
    }

    private boolean executeTwoPL(int txnId, List<String> keys, TransactionTemplate template) {
        if (!lockManager.acquireAll(keys)) {
            return false;
        }

        try {
            Transaction txn = new Transaction(txnId, keys, db);
            txn.begin();
            template.execute(txn, keys);
            txn.applyWrites();
            txn.markCommitted();
            responseTimes.add(txn.getResponseTimeMs());
            responseTimesByTemplate.get(template.getName()).add(txn.getResponseTimeMs());
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            lockManager.releaseAll(keys);
        }
    }

    private boolean executeOCC(int txnId, List<String> keys, TransactionTemplate template) {
        try {
            Set<Integer> ignoreTxns = occValidator.snapshotFinished();
            Transaction txn = new Transaction(txnId, keys, db);
            txn.begin();
            template.execute(txn, keys);

            if (!occValidator.validate(txn, ignoreTxns)) {
                return false;
            }

            // Write phase outside validation lock
            txn.applyWrites();
            txn.markCommitted();
            occValidator.markFinished(txnId);
            responseTimes.add(txn.getResponseTimeMs());
            responseTimesByTemplate.get(template.getName()).add(txn.getResponseTimeMs());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public double getAvgResponseTimeMs() {
        if (responseTimes.isEmpty()) return 0;
        return responseTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }

    public List<Double> getResponseTimes() { return new ArrayList<>(responseTimes); }
    public int getTotalCommitted() { return totalCommitted.get(); }
    public int getTotalRetries() { return totalRetries.get(); }
    public Map<String, List<Double>> getResponseTimesByTemplate() { return responseTimesByTemplate; }
}
