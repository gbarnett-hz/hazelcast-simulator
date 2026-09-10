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
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.cp.helpers.CPMapPartitioned;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Drives concurrent load against a {@link CPMapPartitioned} (CP) and a plain {@link IMap} (AP) from the same
 * worker threads, to create contention on the operation threads shared between the CP and AP subsystems. The two
 * maps use independent, non-overlapping key domains: the contention this test is after is at the operation-thread
 * / resource level, not at the level of shared data.
 */
public class CPApContentionTest extends HazelcastTest {

    private static final Integer VALUE = 0;

    // --- CP (CPMapPartitioned) key domain ---
    // size of the CP key domain; keys are the ids [0, cpKeyCount)
    public int cpKeyCount = 1_000_000;
    // total length in characters/bytes of each generated CP key (a zero-padded decimal index)
    public int cpKeySize = 32;
    // number of CP groups to shard across. Prime by default: CPMapPartitioned
    // note that 31 is selected as it's < vcpus on a c5.9xlarge, so each CPGroup (should) map-to a distinct
    // operation thread
    public int partitionCount = 31;
    // number of threads used to parallelize the CP keyspace preload
    public int preloadThreads = 8;
    // how often (in milliseconds) the preload emits a progress log line, useful for large loads
    public long preloadProgressLogIntervalMs = 15_000;

    // --- AP (IMap) key domain ---
    // size of the AP key domain; keys are the longs [0, mapKeyCount)
    public long mapKeyCount = 1_000_000;

    private Function<String, Integer> cpGetter;
    private BiConsumer<String, Integer> cpSetter;
    private IMap<Long, Integer> apMap;
    private IList<long[]> operationCounts;

    private String cpKeyForIndex(int index) {
        return String.format("%0" + cpKeySize + "d", index);
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

    @Setup
    public void setup() {
        if (cpKeyCount <= 0) {
            throw new IllegalArgumentException("cpKeyCount must be > 0, was " + cpKeyCount);
        }
        int minKeySize = Integer.toString(cpKeyCount - 1).length();
        if (cpKeySize < minKeySize) {
            throw new IllegalArgumentException("cpKeySize must be >= " + minKeySize
                    + " digits to represent cpKeyCount " + cpKeyCount + ", was " + cpKeySize);
        }
        if (mapKeyCount <= 0) {
            throw new IllegalArgumentException("mapKeyCount must be > 0, was " + mapKeyCount);
        }
        if (!isPrime(partitionCount)) {
            logger.warn(name + ": partitionCount " + partitionCount + " is not prime; a prime "
                    + "partitionCount is recommended for a more even key distribution");
        }

        CPMapPartitioned<String, Integer> partitioned = new CPMapPartitioned<>(targetInstance, name + "-cp", partitionCount);
        cpGetter = partitioned::get;
        cpSetter = partitioned::set;

        apMap = targetInstance.getMap(name + "-ap");

        operationCounts = targetInstance.getList(name + "Report");
    }

    // note: this is used to bound the storage before the test runs so we remove that variable
    // for snapshotting it means that we're generally communicating a 'full' snapshot per snapshot
    // event rather than some intermediate size. CP and AP preloads run concurrently since they
    // stress independent resources.
    @Prepare(global = true)
    public void prepare() {
        ThreadSpawner spawner = new ThreadSpawner(name);
        spawner.spawn(this::preloadCp);
        spawner.spawn(this::preloadAp);
        spawner.awaitCompletion();
    }

    private void preloadCp() {
        AtomicLong preloadedCount = new AtomicLong();
        AtomicLong nextLogAtMillis = new AtomicLong(System.currentTimeMillis() + preloadProgressLogIntervalMs);

        ThreadSpawner spawner = new ThreadSpawner(name + "-cp-preload");
        int shardSize = (cpKeyCount + preloadThreads - 1) / preloadThreads;
        for (int t = 0; t < preloadThreads; t++) {
            int start = t * shardSize;
            int end = Math.min(start + shardSize, cpKeyCount);
            spawner.spawn(() -> {
                for (int i = start; i < end; i++) {
                    cpSetter.accept(cpKeyForIndex(i), VALUE);
                    long count = preloadedCount.incrementAndGet();

                    long logAt = nextLogAtMillis.get();
                    long now = System.currentTimeMillis();
                    if (now >= logAt && nextLogAtMillis.compareAndSet(logAt, now + preloadProgressLogIntervalMs)) {
                        logger.info(name + ": CP key loaded: " + count + " / " + cpKeyCount);
                    }
                }
            });
        }
        spawner.awaitCompletion();

        logger.info(name + ": preloaded " + cpKeyCount + " CP keys of " + cpKeySize + " bytes each");
    }

    private void preloadAp() {
        Streamer<Long, Integer> streamer = StreamerFactory.getInstance(apMap);
        for (long key = 0; key < mapKeyCount; key++) {
            streamer.pushEntry(key, VALUE);
        }
        streamer.await();

        logger.info(name + ": preloaded " + mapKeyCount + " AP keys");
    }

    @TimeStep(prob = 0.25)
    public void cpGet(ThreadState state) {
        cpGetter.apply(state.randomCpKey());
        state.cpGetCount++;
    }

    @TimeStep(prob = 0.25)
    public void cpSet(ThreadState state) {
        cpSetter.accept(state.randomCpKey(), VALUE);
        state.cpSetCount++;
    }

    @TimeStep(prob = 0.25)
    public void apGet(ThreadState state) {
        apMap.get(state.randomApKey());
        state.apGetCount++;
    }

    @TimeStep(prob = 0.25)
    public void apSet(ThreadState state) {
        apMap.set(state.randomApKey(), VALUE);
        state.apSetCount++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounts.add(new long[]{state.cpGetCount, state.cpSetCount, state.apGetCount, state.apSetCount});
    }

    @Verify(global = true)
    public void verify() {
        long totalCpGets = 0;
        long totalCpSets = 0;
        long totalApGets = 0;
        long totalApSets = 0;
        for (long[] counts : operationCounts) {
            totalCpGets += counts[0];
            totalCpSets += counts[1];
            totalApGets += counts[2];
            totalApSets += counts[3];
        }
        logger.info(name + ": totalCpGets=" + totalCpGets + " totalCpSets=" + totalCpSets
                + " totalApGets=" + totalApGets + " totalApSets=" + totalApSets
                + " from " + operationCounts.size() + " worker threads");

        // sanity-check a handful of sample keys on both maps
        int[] cpSampleIndexes = {0, cpKeyCount / 2, cpKeyCount - 1};
        for (int index : cpSampleIndexes) {
            Integer value = cpGetter.apply(cpKeyForIndex(index));
            assertNotNull(name + ": expected preloaded CP key at index " + index + " to be present", value);
        }

        long[] apSampleIndexes = {0, mapKeyCount / 2, mapKeyCount - 1};
        for (long index : apSampleIndexes) {
            Integer value = apMap.get(index);
            assertNotNull(name + ": expected preloaded AP key at index " + index + " to be present", value);
        }
    }

    public class ThreadState extends BaseThreadState {
        long cpGetCount;
        long cpSetCount;
        long apGetCount;
        long apSetCount;

        String randomCpKey() {
            return cpKeyForIndex(randomInt(cpKeyCount));
        }

        long randomApKey() {
            return randomLong(mapKeyCount);
        }
    }
}
