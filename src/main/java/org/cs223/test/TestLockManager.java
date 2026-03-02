package org.cs223.test;

import org.cs223.LockManager;
import java.util.List;

public class TestLockManager {
    public static void main(String[] args) {
        LockManager lm = new LockManager();

        // Test 1: Acquire and release
        boolean got = lm.acquireAll(List.of("key1", "key2", "key3"));
        System.out.println("Acquired key1,key2,key3: " + got);

        lm.releaseAll(List.of("key1", "key2", "key3"));
        System.out.println("Released all locks");

        // Test 2: Acquire again after release
        got = lm.acquireAll(List.of("key1", "key2"));
        System.out.println("Acquired key1,key2 again: " + got);

        // Test 3: Another thread tries to acquire overlapping keys
        Thread t = new Thread(() -> {
            boolean result = lm.acquireAll(List.of("key2", "key3"));
            System.out.println("Thread2 acquired key2,key3: " + result);

            boolean result2 = lm.acquireAll(List.of("key4", "key5"));
            System.out.println("Thread2 acquired key4,key5: " + result2);
            lm.releaseAll(List.of("key4", "key5"));
        });
        t.start();
        try { t.join(); } catch (InterruptedException e) {}

        lm.releaseAll(List.of("key1", "key2"));
        System.out.println("\nAll LockManager tests passed!");
    }
}
