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
import com.hazelcast.simulator.tests.cp.helpers.CpMapOperationCounter;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.Random;
import static org.junit.Assert.assertTrue;

public class CPMapSimpleTest extends HazelcastTest {
  public int keys = 1;
  // number of possible values
  public int valueCount = 100;
  // size in bytes for each key's associated value
  public int valueSizeBytes = 100;
  // name of map
  public String mapName;

  private byte[][] values;

  private CPMap<Long, byte[]> map;

  private IList<CpMapOperationCounter> operationCounterList;

  @Setup
  public void setup() {
    values = createValues();
    map = targetInstance.getCPSubsystem().getMap(mapName);
    // all clients (and threads) dump their stats into this list
    operationCounterList = targetInstance.getList(name + "-report");
  }

  // populates the key space with a random value from values
  // before we begin
  @Prepare(global = true)
  public void prepare() {
    Random random = new Random(0);
    for (long i = 0; i < keys; i++) {
      map.set(i, values[random.nextInt(values.length)]);
    }
  }

  private byte[][] createValues() {
    byte[][] valuesArray = new byte[valueCount][valueSizeBytes];
    Random random = new Random(0);
    for (int i = 0; i < valuesArray.length; i++) {
      valuesArray[i] = GeneratorUtils.generateByteArray(random, valueSizeBytes);
    }
    return valuesArray;
  }

  @TimeStep(prob = 1)
  public void put(ThreadState state) {
    map.put(state.randomKey(), state.randomValue());
    state.operationCounter.putCount++;
  }

  @TimeStep(prob = 0)
  public void get(ThreadState state) {
    map.get(state.randomKey());
    state.operationCounter.getCount++;
  }

  @AfterRun
  public void afterRun(ThreadState state) {
    operationCounterList.add(state.operationCounter);
  }

  @Verify(global = true)
  public void verify() {
    // summation of all stats, all threads, all clients
    CpMapOperationCounter total = new CpMapOperationCounter();
    for (CpMapOperationCounter operationCounter : operationCounterList) {
      total.add(operationCounter);
    }
    logger.info(name + ": " + total + " from " + operationCounterList.size() + " worker threads");

    // basic verification
    int entriesCount = 0;
    for (long key = 0; key < keys; key++) {
      byte[] v = map.get(key);
      if (v != null) {
        entriesCount++;
      }
    }

    logger.info(name + ":  CP Map " + mapName + " entries count: " + entriesCount);
    assertTrue("CP Map " + map.getName() + " doesn't contain any of expected items.", entriesCount > 0);
  }

  public class ThreadState extends BaseThreadState {
    final CpMapOperationCounter operationCounter = new CpMapOperationCounter();

    public long randomKey() {
      return randomLong(keys);
    }

    public byte[] randomValue() {
      return values[randomInt(valueCount)]; // [0, values)
    }
  }
}
