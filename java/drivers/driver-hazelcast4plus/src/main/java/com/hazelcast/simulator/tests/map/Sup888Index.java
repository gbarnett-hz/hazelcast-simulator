/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.Predicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Collection;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArrays;

public class Sup888Index extends HazelcastTest {
	public static class Dummy {
		int epochSeconds;
		String deskCode; // guess on this one
		byte[] data;

		public Dummy(int epochSeconds, String deskCode, int dataSizeBytes) {
			this.epochSeconds = epochSeconds;
			this.deskCode = deskCode;
			this.data = new byte[dataSizeBytes];
			ThreadLocalRandom.current().nextBytes(data);
		}

		public void setEpochSeconds(int epochSeconds) {
			this.epochSeconds = epochSeconds;
		}

		public int getEpochSeconds() {
			return epochSeconds;
		}

		public void setDeskCode(String deskCode) {
			this.deskCode = deskCode;
		}

		public String getDeskCode() {
			return deskCode;
		}

		public void setData(byte[] data) {
			this.data = data;
		}

		public byte[] getData() {
			return data;
		}
	}

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int dataSizeBytes = 1024;

    private IMap<Long, Dummy> map;
    private List<Dummy> values;
    private final int midPoint = keyDomain / 2;
    private final Predicate gt = Predicates.greaterThan("epochSeconds", midPoint);
    private final Predicate lt = Predicates.lessThan("epochSeconds", midPoint + 100);
    private final Predicate conj = Predicates.and(gt, lt);

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        values = generateDummyInstances(valueCount, dataSizeBytes);
    }

    List<Dummy> generateDummyInstances(int valueCount, int dataSizeBytes) {
    	List<Dummy> dummyList = new ArrayList<>(valueCount);
    	for (int i = 0; i < valueCount; i++) {
    		int epochSeconds =ThreadLocalRandom.current().nextInt(keyDomain);
    		String deskCode = "d" + ThreadLocalRandom.current().nextInt(keyDomain);
    		dummyList.add(new Dummy(epochSeconds, deskCode, dataSizeBytes));
    	}
    	return dummyList;
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Long, Dummy> streamer = StreamerFactory.getInstance(map);
        for (long key = 0; key < keyDomain; key++) {
            Dummy value = values.get(ThreadLocalRandom.current().nextInt(valueCount));
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = 0)
    public Dummy get(ThreadState state) {
        return map.get(state.randomKey());
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        map.delete(state.randomKey());
    }

    @TimeStep(prob = 0)
    public Collection<?> queryEpoch(ThreadState state) {
	return map.values(conj);
    }

    @TimeStep(prob = 0)
    public Collection<?> deskCode(ThreadState state) {
        String dc = "d" + ThreadLocalRandom.current().nextInt(keyDomain);
	Predicate p = Predicates.equal("deskCode", dc);
	return map.values(p);
    }

    public class ThreadState extends BaseThreadState {
        private long randomKey() {
            return randomLong(keyDomain);
        }

        private Dummy randomValue() {
            return values.get(randomInt(values.size()));
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
