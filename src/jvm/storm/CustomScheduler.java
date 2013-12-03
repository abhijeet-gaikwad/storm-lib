package storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class CustomScheduler implements IScheduler {

	private String topologyName;
	private Map<String,String> execSupMap; //executor to host mapping!
	//private String supervisor;
	//private String executor;
	
	private static final String SP = "spout";
	private static final String HOST = "hostname"; 

	@Override
	public void prepare(Map conf) {
	  //Take this from Nimbus conf!!
      //topologyName = (String) conf.get(Config.STORM_SCHEDULER1_TOPOLOGY_NAME);
      //execSupMap = (Map<String, String>) conf.get(Config.STORM_SCHEDULER1_TOPOLOGY_NAME);
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
	  	System.out.println("DemoScheduler: begin scheduling");
	  	
	  	if (null != cluster.getWeakHosts()) {
	  		System.out.println(cluster.getWeakHosts());
	  	}
	  
	  	new EvenScheduler().schedule(topologies, cluster);
	  	
	  	Map<String,List<ExecutorDetails>> weakH = cluster.getWeakHosts();
	  	Set<String> keys = weakH.keySet();
	  	
	  	Collection<TopologyDetails> topos = topologies.getTopologies();
	  	
	  	for (String key : keys) {
	  		for (ExecutorDetails execDet : weakH.get(key)) {
	  			for (TopologyDetails topology : topos) {
	  				if (topology.getExecutors().contains(execDet)) {
	  					cluster.rmExecutorAssignment(topology.getId(), execDet);
	  				}
	  			}
	  		}
	  	}
	  		  	
/*	  	Map<String,List<ExecutorDetails>> weakH = cluster.getWeakHosts();
	  	
	  	Map<String, SupervisorDetails> sup = cluster.getSupervisors();
	  	
	  	List<SupervisorDetails> supL = new ArrayList<SupervisorDetails>();
	  	
	  	//get all the supervisors running on faulty hosts
	  	Set<String> keys = sup.keySet();
	  	for (String key : keys) {
	  		if (weakH.containsKey(sup.get(key).getHost())) {
	  			supL.add(sup.get(key));
	  		}
	  	}
	  		  	
	  	Collection<TopologyDetails> topos = topologies.getTopologies();
	  	//schedule!
	  	for (TopologyDetails topology : topos) {
  			List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
  			availableSlots.removeAll(supL);
  			Collection<ExecutorDetails> executors = cluster.getUnassignedExecutors(topology);
  			
	  	}*/
	  	
        // Gets the topology which we want to schedule
        //TopologyDetails topology = topologies.getByName(topologyName);

        // make sure the special topology is submitted,
        /*if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
            	System.out.println("Our special topology DOES NOT NEED scheduling.");
            } else {
            	System.out.println("Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                
                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName(topologyName).getId());
                if (currentAssignment != null) {
                	System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                	System.out.println("current assignments: {}");
                }
                
                if (!componentToExecutors.containsKey(execSupMap.get(SP))) {
                	System.out.println("Our special-spout DOES NOT NEED scheduling.");
                } else {
                    System.out.println("Our special-spout needs scheduling.");

                    List<ExecutorDetails> executors = componentToExecutors.get(execSupMap.get(SP));

                    // find out the our "special-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    List<SupervisorDetails> specialSupervisor = new ArrayList<SupervisorDetails>();
                    for (SupervisorDetails supervisor : supervisors) {
                        String h = supervisor.getHost();
                    
                        if (h.equals(execSupMap.get(HOST))) {
                            specialSupervisor.add(supervisor);
                        }
                    }

                    // found the special supervisor
                    if (specialSupervisor != null) {
                    	System.out.println("Found the special-supervisor");
                    	for (SupervisorDetails ss: specialSupervisor) {
                          List<WorkerSlot> availableSlots = cluster.getAvailableSlots(ss);
                                                  
                          // re-get the aviableSlots
                          availableSlots = cluster.getAvailableSlots(ss);
                          if (availableSlots.isEmpty()) {
                        	  continue;
                          }

                          // since it is just a demo, to keep things simple, we assign all the
                          // executors into one slot.
                          cluster.assign(availableSlots.get(0), topology.getId(), executors);
                          System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
                    	}
                    	
                    	// if there is no available slots on this supervisor, free some.
                        // TODO for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(ss)) {
                                cluster.freeSlot(new WorkerSlot(ss.getId(), port));
                            }
                        }

                    	
                    } else {
                    	System.out.println("There is no supervisor named special-supervisor!!!");
                    }
                }
            }
        }*/
        
        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
	}

}
