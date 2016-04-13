package project.systems.distributed.jhalakpatel;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.nio.charset.Charset;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

public class RoundRobinCommSystem {

	/* class private variables */
	private static int count;

	public static int numNodes = 32;

	public static int totalValues = 0;

	public static SharedMemory SharedMem = new SharedMemory();

	public static final ActorRef[] NodeRef = new ActorRef[numNodes];
	
	public 	static String valueString = "download";

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws FileNotFoundException {

		final ActorSystem system = ActorSystem.create("RoundRobinCommSystem");
		ActorRef pred = system.actorOf(Props.create(ServerNode.class), "predNode");
		ActorRef succ = system.actorOf(Props.create(ServerNode.class), "succNode");
		ArrayList<ArrayList<String>> valueDistribution;

		for (int i = 0; i < numNodes; i++) {

			// get the node reference for the current actor node
			NodeRef[i] = system.actorOf(Props.create(ServerNode.class, i, 0, pred, succ), "ServerNode." + i);
			//System.out.println(NodeRef[i].path().name() + " : ServerNode Created");
		}

		System.out.println("-----------------------------------------------");
		System.out.println("Main : Starting Communication");
		System.out.println("-----------------------------------------------");

		// Testing Consistent Hashing
		valueDistribution = ConsistentHash();

		for (int i = 0; i < numNodes; i++) {
			NodeRef[i].tell(new Message.DistributeData(valueDistribution.get(i)), ActorRef.noSender());
		}

		System.out.println("<<Looking value : "+valueString+">>");
		System.out.println();
		
		// start lookup
		NodeRef[0].tell(new Message.LookUp(valueString, NodeRef[numNodes - 1], NodeRef[1]), ActorRef.noSender());

		// wait for the node to be found
		while (SharedMem.lookupDone == false) {
		}
		;

		int nodeToKill = SharedMem.lookupNode;

		// get ready for the next lookup
		SharedMem.lookupDone = false;

		// get ready for the next update
		SharedMem.updateDone = false;
		
		// Node which has the value
		System.out.println("Value found   : ServerNode." + SharedMem.lookupNode);

		
		System.out.println("------<<------- Leaving the network : ServerNode." + SharedMem.lookupNode);

		// Kill the node
		NodeRef[nodeToKill].tell(new Message.Leave(NodeRef[nodeToKill - 1], NodeRef[nodeToKill + 1]),
				ActorRef.noSender());

		while (SharedMem.updateDone == false) {
		}

		//System.out.println("Staring Next Lookup");

		// start look up again
		NodeRef[0].tell(new Message.LookUp(valueString, NodeRef[numNodes - 1], NodeRef[1]), ActorRef.noSender());

		// wait for the node to be found
		while (SharedMem.lookupDone == false) {
		}

		// Node which has the value
		System.out.println("Value found   : ServerNode." + SharedMem.lookupNode);

		// create the node again
		NodeRef[nodeToKill] = system.actorOf(Props.create(ServerNode.class, nodeToKill, 0, pred, succ),
				"ServerNode." + nodeToKill);

		System.out.println("------>>------- Joining the network : ServerNode." + nodeToKill);
		
		// Join the node
		NodeRef[nodeToKill].tell(new Message.Join(NodeRef[nodeToKill - 1], NodeRef[nodeToKill + 1]),
				ActorRef.noSender());

		// reset the lookupDone
		SharedMem.lookupDone = false;
		
		// perform the next lookup 
		NodeRef[0].tell(new Message.LookUp(valueString, NodeRef[numNodes - 1], NodeRef[1]), ActorRef.noSender());
		
		// wait for the node to be found
		while (SharedMem.lookupDone == false) {
		}
		
		// Node which has the value
		System.out.println("Value found   : ServerNode." + SharedMem.lookupNode);
		
		// Kill the system
		system.shutdown();
	}

	public static ArrayList<ArrayList<String>> ConsistentHash() throws FileNotFoundException {
		ArrayList<ArrayList<String>> valueDistribution = new ArrayList<ArrayList<String>>();
		List<String> values = new ArrayList<String>();
		Charset charsetFMT = Charset.forName("UTF-8");

		File file = new File("/Users/jhalakpatel/Desktop/dictionary.txt");
		Scanner input = new Scanner(file);

		while (input.hasNext()) {
			//System.out.println(input.nextLine());
			values.add(input.nextLine());
			totalValues++;
		}
		input.close();
		
//		for (int i=0; i<totalValues; i++){
//			values.add("value"+i);
//		}

		for (int i = 0; i < numNodes; i++) {
			ArrayList<String> serverBucket = new ArrayList<String>();
			valueDistribution.add(serverBucket);
		}


		for (int i = 0; i < totalValues; i++) {
			int bucket = Hashing.consistentHash(Hashing.md5().hashString(values.get(i), charsetFMT),
					valueDistribution.size());
			if ((values.get(i)).equals(valueString)){
				System.out.println("ServerNode : "+bucket);
			}
			valueDistribution.get(bucket).add(values.get(i));
		}

		return valueDistribution;
	}
}
