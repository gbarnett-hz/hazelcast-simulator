/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.cp;

import com.hazelcast.collection.IList;
import com.hazelcast.cp.CPMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.cp.helpers.CPMapPartitioned;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Compares get/set scalability of a single {@link CPMap} (hosted by one CP group) against a {@link CPMapPartitioned}
 * (sharded across {@code partitionCount} CP groups), over a large, memory-cheap keyspace of unique, fixed-length
 * 34-byte string keys.
 * <p>
 * Keys are never materialized as a stored collection: each key is deterministically derived from an index in
 * {@code [0, keyCount)} on demand (see {@link #keyForIndex(int)}), which guarantees uniqueness by construction and
 * keeps memory use independent of {@code keyCount}.
 */
public class CPMapScalabitityComparisonTest extends HazelcastTest {

    public enum Mode {
        SINGLE,
        PARTITIONED
    }

    // "MID" (3 chars) + a zero-padded decimal index (31 chars) = 34 bytes total, well within the int range.
    private static final int KEY_LENGTH = 34;
    private static final Integer VALUE = 0;

    // which implementation to exercise
    public Mode mode = Mode.SINGLE;
    // size of the key domain; keys are the ids [0, keyCount)
    public int keyCount = 1_000_000;
    // number of CP groups to shard across; only used when mode == PARTITIONED. Prime by default: CPMapPartitioned
    // routes keys via hash(key) % partitionCount, and since these keys only differ in a numeric suffix, a prime
    // modulus avoids the clustering that a composite (e.g. power-of-two) modulus can introduce.
    //
    // NOTE this is unrelated to, and cannot control, how CP groups land on a member's CP-group operation
    // threads (cpGroupId % operationThreads). CP group ids are assigned via
    // RaftInvocationManager.generateRandomGroupIndex() as max(existingGroupIds) + random(0..9999) -- i.e. NOT
    // consecutive -- so groupId % operationThreads is effectively uniform random per group, independent of
    // whether partitionCount is prime or how it relates to operationThreads. Expect some of this test's CP
    // groups to share an operation thread purely by chance (a classic balls-into-bins/birthday-paradox effect),
    // varying from run to run; see the expected-collision estimate logged in setup().
    public int partitionCount = 31;
    // number of CP-group operation (partition) threads on the target machines, e.g. one per vCPU. Used only to
    // estimate expected operation-thread reuse for partitionCount, per the note above.
    public int operationThreads = 36;
    // number of threads used to parallelize the one-off keyspace preload
    public int preloadThreads = 8;

    private Function<String, Integer> getter;
    private BiConsumer<String, Integer> setter;
    private BiFunction<String, Integer, Integer> putIfAbsenter;
    private IList<long[]> operationCounts;

    private static String keyForIndex(int index) {
        return String.format("MID%031d", index);
    }

    private static boolean isPrime(int n) {
        if (n < 2) {
            return false;
        }
        for (int i = 2; (long) i * i <= n; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

    // Expected number of distinct operation threads used when partitionCount CP groups are thrown at random
    // (see the partitionCount field comment) into operationThreads bins: the classic balls-into-bins occupancy
    // expectation, operationThreads * (1 - ((operationThreads - 1) / operationThreads) ^ partitionCount).
    private static double expectedDistinctOperationThreads(int partitionCount, int operationThreads) {
        double probThreadUnused = Math.pow((operationThreads - 1.0) / operationThreads, partitionCount);
        return operationThreads * (1 - probThreadUnused);
    }

    @Setup
    public void setup() {
        if (keyCount <= 0) {
            throw new IllegalArgumentException("keyCount must be > 0, was " + keyCount);
        }

        switch (mode) {
            case SINGLE:
                CPMap<String, Integer> cpMap = targetInstance.getCPSubsystem().getMap(name);
                getter = cpMap::get;
                setter = cpMap::set;
                putIfAbsenter = cpMap::putIfAbsent;
                break;
            case PARTITIONED:
                if (!isPrime(partitionCount)) {
                    logger.warn(name + ": partitionCount " + partitionCount + " is not prime; a prime "
                            + "partitionCount is recommended for a more even key distribution");
                }
                if (partitionCount > operationThreads) {
                    logger.warn(name + ": partitionCount " + partitionCount + " exceeds operationThreads "
                            + operationThreads + "; some CP groups are guaranteed to share an operation thread");
                } else {
                    double expectedThreads = expectedDistinctOperationThreads(partitionCount, operationThreads);
                    logger.info(name + ": " + partitionCount + " CP groups over " + operationThreads
                            + " operation threads are expected to land on ~" + String.format("%.1f", expectedThreads)
                            + " distinct threads (CP group ids are randomly spaced, so some sharing is expected "
                            + "regardless of partitionCount, and will vary run to run)");
                }
                CPMapPartitioned<String, Integer> partitioned =
                        new CPMapPartitioned<>(targetInstance, name, partitionCount);
                getter = partitioned::get;
                setter = partitioned::set;
                putIfAbsenter = partitioned::putIfAbsent;
                break;
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }

        operationCounts = targetInstance.getList(name + "Report");
    }

    // note: this is used to bound the storage before the test runs so we remove that variable
    // for snapshotting it means that we're generally communicating a 'full' snapshot per snapshot 
    // event rather than some intermediate size.
    @Prepare(global = true)
    public void prepare() {
        logger.info(name + ": preloading " + keyCount + " keys, approximate dataset size: "
                + humanReadableBytes(estimatedDatasetSizeBytes()));

        ThreadSpawner spawner = new ThreadSpawner(name);
        int shardSize = (keyCount + preloadThreads - 1) / preloadThreads;
        for (int t = 0; t < preloadThreads; t++) {
            int start = t * shardSize;
            int end = Math.min(start + shardSize, keyCount);
            spawner.spawn(() -> {
                for (int i = start; i < end; i++) {
                    setter.accept(keyForIndex(i), VALUE);
                }
            });
        }
        spawner.awaitCompletion();
        logger.info(name + ": preloaded " + keyCount + " keys of " + KEY_LENGTH + " bytes each");
    }

    // rough approximation of the raw dataset size: keyCount * (key bytes + value bytes), ignoring per-entry
    // storage/object overhead of the underlying CP data structures.
    private long estimatedDatasetSizeBytes() {
        return (long) keyCount * KEY_LENGTH + (long) keyCount * Integer.BYTES;
    }

    private static String humanReadableBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        String[] units = {"KB", "MB", "GB", "TB"};
        double value = bytes;
        int unitIndex = -1;
        do {
            value /= 1024;
            unitIndex++;
        } while (value >= 1024 && unitIndex < units.length - 1);
        return String.format("%.2f %s (%d bytes)", value, units[unitIndex], bytes);
    }

    @TimeStep(prob = 0.5)
    public void get(ThreadState state) {
        getter.apply(state.randomKey());
        state.getCount++;
    }

    @TimeStep(prob = 0.5)
    public void set(ThreadState state) {
        setter.accept(state.randomKey(), VALUE);
        state.setCount++;
    }

    @TimeStep(prob = 0)
    public void putIfAbsent(ThreadState state) {
        putIfAbsenter.apply(state.randomKey(), VALUE);
        state.putIfAbsentCount++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounts.add(new long[]{state.getCount, state.setCount, state.putIfAbsentCount});
    }

    @Verify(global = true)
    public void verify() {
        long totalGets = 0;
        long totalSets = 0;
        long totalPutIfAbsents = 0;
        for (long[] counts : operationCounts) {
            totalGets += counts[0];
            totalSets += counts[1];
            totalPutIfAbsents += counts[2];
        }
        logger.info(name + ": totalGets=" + totalGets + " totalSets=" + totalSets
                + " totalPutIfAbsents=" + totalPutIfAbsents
                + " from " + operationCounts.size() + " worker threads");

        // sanity-check a handful of sample keys rather than scanning the whole keyspace
        int[] sampleIndexes = {0, keyCount / 2, keyCount - 1};
        for (int index : sampleIndexes) {
            Integer value = getter.apply(keyForIndex(index));
            assertNotNull(name + ": expected preloaded key at index " + index + " to be present", value);
        }
    }

    public class ThreadState extends BaseThreadState {
        long getCount;
        long setCount;
        long putIfAbsentCount;

        String randomKey() {
            return keyForIndex(randomInt(keyCount));
        }
    }
}
