package org.cs223;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {
    private final ConcurrentHashMap<String, ReentrantLock> lockTable = new ConcurrentHashMap<>();

    private ReentrantLock getLock(String key) {
        return lockTable.computeIfAbsent(key, k -> new ReentrantLock());
    }

    /**
     * Try to acquire all locks for the given keys.
     * Keys are sorted to ensure consistent ordering.
     * If any lock is unavailable, release all and return false.
     */
    public boolean acquireAll(List<String> keys) {
        List<String> sorted = new ArrayList<>(keys);
        Collections.sort(sorted);

        List<String> acquired = new ArrayList<>();
        for (String key : sorted) {
            ReentrantLock lock = getLock(key);
            if (lock.tryLock()) {
                acquired.add(key);
            } else {
                // Can't get this lock, release all acquired locks
                for (String held : acquired) {
                    getLock(held).unlock();
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Release all locks for the given keys.
     */
    public void releaseAll(List<String> keys) {
        for (String key : keys) {
            ReentrantLock lock = lockTable.get(key);
            if (lock != null && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
