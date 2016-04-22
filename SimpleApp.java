package project.systems.distributed.jhalakpatel;

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.Props;

public class SimpleApp {

	public int nodeToKill;
	public List<Integer> killedNodesList = new ArrayList<Integer>();

	public void lookUpFunc(String valueString, int sid) {
		int level = sid % ViceroySystem.numLevels;
		int posInLevel = sid / ViceroySystem.numLevels;
		
		System.out.println();
		System.out.println("=========### ServerNode." + sid + " Initiating Look Up ###========= ");
		System.out.println();
		
		ViceroySystem.NodeRef[level][posInLevel].tell(new Message.LookUp(valueString, "moveUp"), ActorRef.noSender());

		// wait for the value to be found on a node
		while (ViceroySystem.SharedMem.lookupDone == false) {
		}
		;

		// get ready for the next lookup
		ViceroySystem.SharedMem.lookupDone = false;

		// get ready for the next update
		ViceroySystem.SharedMem.updateDone = false;

		// Node which has the value
		// System.out.println("Value found : ServerNode." +
		// ViceroySystem.SharedMem.lookupNode);
	}

	public void LeaveFunc() {

		System.out.println("------<<------- Leaving the network : ServerNode." + ViceroySystem.SharedMem.lookupNode);
		List<Integer> participatingNodesLocal = new ArrayList<Integer>(ViceroySystem.participatingNodes);
		nodeToKill = ViceroySystem.SharedMem.lookupNode;
		int level = nodeToKill % ViceroySystem.numLevels;
		int posInLevel = nodeToKill / ViceroySystem.numLevels;
		int killCount = 0;

		// Kill the node
		ViceroySystem.NodeRef[level][posInLevel].tell(new Message.Leave(), ActorRef.noSender());
		while (ViceroySystem.SharedMem.updateDone == false) {
		}

		ViceroySystem.SharedMem.updateDone = false;

		killedNodesList.add(nodeToKill);

		// kill multiple Nodes
		for (int nodeKillId : participatingNodesLocal) {
			level = nodeKillId % ViceroySystem.numLevels;
			posInLevel = nodeKillId / ViceroySystem.numLevels;
			if (nodeKillId == nodeToKill)
				continue;
			System.out.println("------<<------- Leaving the network : ServerNode." + nodeKillId);
			ViceroySystem.NodeRef[level][posInLevel].tell(new Message.Leave(), ActorRef.noSender());
			while (ViceroySystem.SharedMem.updateDone == false) {
			}
			ViceroySystem.SharedMem.updateDone = false;
			killedNodesList.add(nodeKillId);
			killCount++;
			// System.out.println("KILL COUNT - "+killCount);
			if (killCount == 4)
				break;
		}
	}

	public void joinFunc() {
		for (int sid : killedNodesList) {
			System.out.println("------>>------- Joining the network : ServerNode." + sid);
			// create the node again
			int level = sid % ViceroySystem.numLevels;
			int posInLevel = sid / ViceroySystem.numLevels;
			ViceroySystem.NodeRef[level][posInLevel] = ViceroySystem.AccaSystem.actorOf(
					Props.create(ServerNode.class, sid, level, posInLevel, null, null, null, null, null),
					"ServerNode." + sid);

			// Join the node
			ViceroySystem.NodeRef[level][posInLevel].tell(new Message.Join(), ActorRef.noSender());

			while (ViceroySystem.SharedMem.updateDone == false) {
			}

			// reset the lookupDone
			ViceroySystem.SharedMem.lookupDone = false;
		}
	}

	public void bringUpTest() {
		lookUpFunc(ViceroySystem.valueString, ViceroySystem.participatingNodes.get(0));
//		LeaveFunc();
//		lookUpFunc(ViceroySystem.valueString, ViceroySystem.participatingNodes.get(6));
//		joinFunc();
//		lookUpFunc(ViceroySystem.valueString, ViceroySystem.participatingNodes.get(6));
	}
}
