package project.systems.distributed.jhalakpatel;

import akka.actor.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.nio.charset.Charset;
import com.google.common.hash.Hashing;

public class ViceroySystem {

	/* class private variables */
	public static int totalValues = 0;
	public static SharedMemory SharedMem = new SharedMemory();
	public static SimpleApp SimpleApp = new SimpleApp();
	public static String valueString = "viceroy";
	public static ArrayList<ArrayList<String>> valueDistribution;
	public static int lookUpCount;

	/* node related data structure */
	public static int maxLevel = 11;
	public static int maxNodes = 11264;
	public static int numNodes;
	public static int numLevels;
	public static int dataStoreNode;
	public static int participatingNodesCount;
	public static int maxNodesAtLevel;
	public static HashMap<Integer, Integer> MapSidToIndex = new HashMap<Integer, Integer>();
	public static List<Integer> participatingNodes = new ArrayList<Integer>();
	public static ActorRef[][] NodeRef;
	public static final ActorSystem AccaSystem = ActorSystem.create("ViceroySystem");

	public static void main(String[] args) throws FileNotFoundException {

		// Build the Array of Participating Nodes - Initially
		for (int i = 0; i < maxNodes; i++) {
			/*
			 * some stupid strategy to get number of participating nodes the
			 * number of nodes
			 */
			if (i % 1 == 0) {
				participatingNodesCount++;
			}
		}

		// get the location parameters updated
		locationParamUpdate();

		// System.out.println("[STATUS] PARAMETER UPDATED");

		// Consistent Hashing Strategy
		valueDistribution = ConsistentHash();

		// System.out.println("[STATUS] HASHING DONE");

		// build the network
		formNetwork();

		// System.out.println("[STATUS] FRAMEWORK UP");

		System.out.println("-----------------------------------------------");
		System.out.println("Main : Starting Communication");
		System.out.println("-----------------------------------------------");

		distributeData();

		// System.out.println("[STATUS] DATA DISTRIBUTED");
		System.out.println();
		System.out.println("========### Looking value : \"" + valueString + "\". Originally Stored at : " + dataStoreNode + " ###========");
		System.out.println();

		/* ================ BASIC TESTING ==================== */
		SimpleApp.bringUpTest();
		/* ================ BASIC TESTING DONE ==================== */
		
		System.out.println("======================================================");
		System.out.println("Final Look Up Count : "+ViceroySystem.lookUpCount);
		System.out.println("======================================================");

	}

	public static ArrayList<ArrayList<String>> ConsistentHash() throws FileNotFoundException {
		ArrayList<ArrayList<String>> valueDistribution = new ArrayList<ArrayList<String>>();
		List<String> values = new ArrayList<String>();
		Charset charsetFMT = Charset.forName("UTF-8");
		List<Integer> sortedParticipatingNodes = new ArrayList<Integer>(participatingNodes);
		HashMap<Integer, Integer> HashMapNodesDesiredToActual = new HashMap<Integer, Integer>();

		Collections.sort(sortedParticipatingNodes);

		File file = new File("/Users/jhalakpatel/Desktop/dictionary.txt");
		Scanner input = new Scanner(file);

		while (input.hasNext()) {
			values.add(input.nextLine());
			totalValues++;
		}
		input.close();

		for (int i = 0; i < numNodes; i++) {
			ArrayList<String> serverBucket = new ArrayList<String>();
			valueDistribution.add(serverBucket);
		}

		for (int i = 0; i < totalValues; i++) {
			int bucketAllocated = 0;
			int bucketDesired = Hashing.consistentHash(Hashing.md5().hashString(values.get(i), charsetFMT),
					valueDistribution.size());

			// store the value in the bucket is the corresponding server exists,
			// if not then store in the next available bucket
			if (participatingNodes.contains(bucketDesired)) {
				bucketAllocated = bucketDesired;
				HashMapNodesDesiredToActual.put(bucketDesired, bucketAllocated);
			} else {
				// look up in the already stored mapping
				if (HashMapNodesDesiredToActual.get(bucketDesired) != null) {
					bucketAllocated = HashMapNodesDesiredToActual.get(bucketDesired);
				} else {
					// if the bucket does not exists, get the next legal bucket
					for (int bucket : sortedParticipatingNodes) {
						if (bucket >= bucketDesired && bucket < numNodes - 1) {
							bucketAllocated = bucket;
							HashMapNodesDesiredToActual.put(bucketDesired, bucketAllocated);
						} else if (bucketDesired == numNodes - 1) {
							bucketAllocated = sortedParticipatingNodes.get(0);
						}
					}
				}
			}

			if ((values.get(i)).equals(valueString)) {
				dataStoreNode = bucketAllocated;
			}
			valueDistribution.get(bucketAllocated).add(values.get(i));
			// System.out.println("Bucket Allocated : " + bucketAllocated + " .
			// Bucket Desired : " + bucketDesired);
		}

		return valueDistribution;
	}

	public static void distributeData() {

		// iterate through the list of participating nodes
		for (int sid : participatingNodes) {
			int level = sid % numLevels;
			int posInLevel = sid / numLevels;
			NodeRef[level][posInLevel].tell(new Message.DistributeData(valueDistribution.get(sid)),
					ActorRef.noSender());
		}
	}

	public static void formNetwork() {

		// iterate through the list of participating nodes
		for (int sid : participatingNodes) {
			int level = sid % numLevels;
			int posInLevel = sid / numLevels;
			NodeRef[level][posInLevel] = AccaSystem.actorOf(
					Props.create(ServerNode.class, sid, level, posInLevel, null, null, null, null, null),
					"ServerNode." + sid);
			System.out.println(NodeRef[level][posInLevel].path().name() + " : ServerNode Created");
		}
	}

	public static void updateFramework() {
		// check if there is need to update the network parameters
		int nodesSupportedOnLowerLevel = (numLevels - 1) * (int) Math.pow(2, (numLevels - 1) - 1);

		if (nodesSupportedOnLowerLevel < 1) {
			System.out.println("[STATUS] No Nodes available. Exiting the system");
			System.exit(0);
		}

		// if (participatingNodes.size() != participatingNodesCount)
		// System.out.println("[ERROR] : count mismatch");

		if (participatingNodesCount <= nodesSupportedOnLowerLevel) {

			System.out.println("[STATUS] : Updating the network");

			// need to update location parameters
			locationParamUpdate();

			// form new network
			formNetwork();

			// redistribute the data
			distributeData();

		} else {
			// System.out.println("[STATUS] : No Need to update the network");
		}
	}

	public static void locationParamUpdate() {
		int currLevel = 1;
		int nodesSupported = 0;

		while (currLevel <= maxLevel) {
			nodesSupported = currLevel * (int) Math.pow(2, currLevel - 1);
			if (participatingNodesCount <= nodesSupported) {
				currLevel++;
				break;
			}
			currLevel++;
		}

		// set the framework parameters
		numLevels = currLevel - 1;
		numNodes = nodesSupported;
		maxNodesAtLevel = (int) Math.ceil(numNodes / numLevels);
		NodeRef = new ActorRef[numLevels][maxNodesAtLevel];

		participatingNodes.clear();
		MapSidToIndex.clear();

		// randomly assign node-ids to the participating nodes out of nodes
		// Supported
		while (participatingNodes.size() < participatingNodesCount) {
			int randomnumber = (int) Math.ceil(Math.random() * nodesSupported - 1);
			boolean found = false;
			for (int i = 0; i < participatingNodes.size(); i++) {
				if (participatingNodes.get(i) == randomnumber) {
					found = true;
					break;
				}
			}

			if (!found) {
				MapSidToIndex.put(randomnumber, participatingNodes.size());
				participatingNodes.add(participatingNodes.size(), randomnumber);
			}
		}
		System.out.println("=============== FrameWork Parameters Updated ================");
		System.out.println("Max Level           :    " + numLevels);
		System.out.println("Nodes Supported     :    " + numNodes);
		System.out.println("Participating Nodes :    " + participatingNodes.size());
		System.out.println("=============================================================");
		System.out.println();
	}
}
