package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.vendors.StubVendorDriver;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DeploymentPlanTest {
    private final ComponentRegistry componentRegistry = new ComponentRegistry();
    private SimulatorAddress agent1;
    private SimulatorAddress agent2;
    private SimulatorAddress agent3;
    private VendorDriver vendorDriver;

    @Before
    public void before() {
        agent1 = componentRegistry.addAgent("192.168.0.1", "192.168.0.1").getAddress();
        agent2 = componentRegistry.addAgent("192.168.0.2", "192.168.0.2").getAddress();
        agent3 = componentRegistry.addAgent("192.168.0.3", "192.168.0.3").getAddress();
        vendorDriver = new StubVendorDriver();
    }

    @Test
    public void dedicatedMemberCountEqualsAgentCount() {
        componentRegistry.assignDedicatedMemberMachines(3);
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(1, "member");

        assertDeploymentPlanWorkerCount(plan, agent1, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void whenNoAgents() {
        new DeploymentPlan(vendorDriver, new ComponentRegistry());
    }

    @Test
    public void whenAgentCountSufficientForDedicatedMembersAndClientWorkers() {
        componentRegistry.assignDedicatedMemberMachines(2);
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry.getAgents())
                .addToPlan(1, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 1);
    }


    @Test(expected = CommandLineExitException.class)
    public void whenAgentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        componentRegistry.assignDedicatedMemberMachines(3);
        new DeploymentPlan(vendorDriver, componentRegistry).addToPlan(1, "javaclient");
    }

    @Test
    public void whenSingleMemberWorker() {
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(1, "member");

        assertDeploymentPlanWorkerCount(plan, agent1, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 0);
    }

    @Test
    public void whenMemberWorkerOverflow() {
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(4, "member");

        assertDeploymentPlanWorkerCount(plan, agent1, 2, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 1, 0);
    }

    @Test
    public void whenSingleClientWorker() {
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(1, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 0, 1);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 0);
    }

    @Test
    public void whenClientWorkerOverflow() {
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(5, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 0, 2);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 2);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 1);
    }

    @Test
    public void whenDedicatedAndMixedWorkers1() {
        componentRegistry.assignDedicatedMemberMachines(1);
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(2, "member")
                .addToPlan(3, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 2, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 2);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 1);
    }

    @Test
    public void whenDedicatedAndMixedWorkers2() {
        componentRegistry.assignDedicatedMemberMachines(2);
        DeploymentPlan plan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(2, "member")
                .addToPlan(3, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 3);
    }

    @Test
    public void whenIncrementalDeployment_addFirstClientWorkerToLeastCrowdedAgent() {
        DeploymentPlan plan1 = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(2, "member");
        for (List<WorkerParameters> workersForAgent : plan1.getWorkerDeployment().values()) {
            componentRegistry.addWorkers(workersForAgent);
        }

        DeploymentPlan plan2 = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(4, "javaclient");

        assertDeploymentPlanWorkerCount(plan2, agent1, 0, 1);
        assertDeploymentPlanWorkerCount(plan2, agent2, 0, 1);
        assertDeploymentPlanWorkerCount(plan2, agent3, 0, 2);

        assertDeploymentPlanSizePerAgent(plan2, agent1, 1);
        assertDeploymentPlanSizePerAgent(plan2, agent2, 1);
        assertDeploymentPlanSizePerAgent(plan2, agent3, 2);
    }

    @Test
    public void whenIncrementalDeployment_withDedicatedMembers_addClientsToCorrectAgents() {
        componentRegistry.assignDedicatedMemberMachines(1);
        DeploymentPlan plan1 = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(2, "member");
        for (List<WorkerParameters> workersForAgent : plan1.getWorkerDeployment().values()) {
            componentRegistry.addWorkers(workersForAgent);
        }

        DeploymentPlan plan2 = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(4, "javaclient");

        assertDeploymentPlanWorkerCount(plan2, agent1, 0, 0);
        assertDeploymentPlanWorkerCount(plan2, agent2, 0, 2);
        assertDeploymentPlanWorkerCount(plan2, agent3, 0, 2);

        assertDeploymentPlanSizePerAgent(plan2, agent1, 0);
        assertDeploymentPlanSizePerAgent(plan2, agent2, 2);
        assertDeploymentPlanSizePerAgent(plan2, agent3, 2);
    }

    private void assertDeploymentPlanWorkerCount(DeploymentPlan plan, SimulatorAddress agentAddress,
                                                 int memberCount, int clientCount) {
        List<WorkerParameters> settingsList = plan.getWorkerDeployment().get(agentAddress);
        assertNotNull(format("Could not find WorkerParameters for Agent %s , workerDeployment: %s",
                agentAddress, plan.getWorkerDeployment()), settingsList);

        int actualMemberWorkerCount = 0;
        int actualClientWorkerCount = 0;
        for (WorkerParameters workerProcessSettings : settingsList) {
            if (workerProcessSettings.getWorkerType().equals("member")) {
                actualMemberWorkerCount++;
            } else {
                actualClientWorkerCount++;
            }
        }
        String prefix = format("Agent %s members: %d clients: %d",
                agentAddress,
                actualMemberWorkerCount,
                actualClientWorkerCount);

        assertEquals(prefix + " (memberWorkerCount)", memberCount, actualMemberWorkerCount);
        assertEquals(prefix + " (clientWorkerCount)", clientCount, actualClientWorkerCount);
    }

    private static void assertDeploymentPlanSizePerAgent(DeploymentPlan plan, SimulatorAddress agentAddress, int expectedSize) {
        Map<SimulatorAddress, List<WorkerParameters>> workerDeployment = plan.getWorkerDeployment();
        List<WorkerParameters> settingsList = workerDeployment.get(agentAddress);
        assertEquals(expectedSize, settingsList.size());
    }


    @Test
    public void testGetVersionSpecs() {
        AgentData agent = new AgentData(1, "127.0.0.1", "127.0.0.1");

        testGetVersionSpecs(singletonList(agent), 1, 0);
    }

    @Test
    public void testGetVersionSpecs_noWorkersOnSecondAgent() {
        AgentData agent1 = new AgentData(1, "172.16.16.1", "127.0.0.1");
        AgentData agent2 = new AgentData(2, "172.16.16.2", "127.0.0.1");

        testGetVersionSpecs(asList(agent1,agent2), 1, 0);
    }

    private void testGetVersionSpecs(List<AgentData> agents, int memberCount, int clientCount) {
        ComponentRegistry componentRegistry = new ComponentRegistry();
        for (AgentData agent : agents) {
            componentRegistry.addAgent(agent.getPublicAddress(), agent.getPrivateAddress());
        }

        vendorDriver.set("VERSION_SPEC","outofthebox");
        DeploymentPlan deploymentPlan = new DeploymentPlan(vendorDriver, componentRegistry)
                .addToPlan(memberCount, "member")
                .addToPlan(clientCount, "javaclient");

        assertEquals(singleton("outofthebox"), deploymentPlan.getVersionSpecs());
    }
}
