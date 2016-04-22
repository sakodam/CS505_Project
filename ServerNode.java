package project.systems.distributed.jhalakpatel;

import akka.actor.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import com.google.common.hash.Hashing;

public class ServerNode extends UntypedActor {
	public int sid;
	public int level;
	public int posInLevel;
	public ActorRef shortLink;
	public ActorRef farLink;
	public ActorRef upLink;
	public ActorRef pred;
	public ActorRef succ;
	public String phase;

	ArrayList<String> serverBucket = new ArrayList<String>();
	Charset charsetFMT = Charset.forName("UTF-8");

	public ServerNode() {
		pred = null;
		succ = null;
		shortLink = null;
		farLink = null;
		upLink = null;
	}

	public boolean moveToTopLevel(String phase) {
		if (level == 0) {
			// System.out.println("[" + getSelf().path().name() + "] Up
			// Traversal Complete. At Top Level");
			return true;
		}

		if (phase == "moveUp") {
			// prepare message to communicate with the next closest server
			if (upLink != null) {
				upLink.tell(new Message.LookUp(ViceroySystem.valueString, "moveUp"), ActorRef.noSender());

				// return false - to stop this node from participating
				return false;
			} else {
				// this is the last upLink node, can not move more upwards
				return true;
			}

		} else {
			return true;
		}
	}

	public int distance(int link1, int link2) {
		return Math.abs(link1 - link2);
	}

	public int difference(int link1, int link2) {
		return link1 - link2;
	}

	public void traverseRing(int serverTarget, String phase) {
		// logic to decide between predecessor and successor

		// System.out.println("Traversing Ring");

		if (phase.equals("traverseRing")) {
			if (difference(serverTarget, sid) > 0) {
				if (difference(serverTarget, sid) < ViceroySystem.numNodes / 2) {
					System.out.println("[" + getSelf().path().name()
							+ "] Traversing Ring : Contacting Successor   ==>   " + succ.path().name());
					succ.tell(new Message.LookUp(ViceroySystem.valueString, "traverseSucc"), ActorRef.noSender());
				} else {
					System.out.println("[" + getSelf().path().name()
							+ "] Traversing Ring : Contacting Predecessor ==>   " + pred.path().name());
					pred.tell(new Message.LookUp(ViceroySystem.valueString, "traversePred"), ActorRef.noSender());
				}
			} else {
				if (difference(serverTarget, sid) < ViceroySystem.numNodes / 2) {
					System.out.println("[" + getSelf().path().name()
							+ "] Traversing Ring : Contacting Predecessor ==>   " + pred.path().name());
					pred.tell(new Message.LookUp(ViceroySystem.valueString, "traversePred"), ActorRef.noSender());
				} else {
					System.out.println("[" + getSelf().path().name()
							+ "] Traversing Ring : Contacting Successor   ==>   " + succ.path().name());
					succ.tell(new Message.LookUp(ViceroySystem.valueString, "traverseSucc"), ActorRef.noSender());
				}
			}
		} else if (phase.equals("traversePred")) {
			System.out.println("[" + getSelf().path().name() + "] Traversing Ring : Contacting Predecessor ==>   "
					+ pred.path().name());
			pred.tell(new Message.LookUp(ViceroySystem.valueString, "traversePred"), ActorRef.noSender());
		} else if (phase.equals("traverseSucc")) {
			System.out.println("[" + getSelf().path().name() + "] Traversing Ring : Contacting Successor   ==>   "
					+ succ.path().name());
			succ.tell(new Message.LookUp(ViceroySystem.valueString, "traverseSucc"), ActorRef.noSender());
		}
	}

	public void traverseButterFly(int serverTarget) {
		// logic to decide between farLink and shortLink
		int farLinkId = 0, shortLinkId = 0;

		if (shortLink != null)
			shortLinkId = Integer.parseInt(shortLink.path().name().substring(11));
		if (farLink != null)
			farLinkId = Integer.parseInt(farLink.path().name().substring(11));

		// System.out.println("Traversing ButterFly");

		// System.out.println("server target : "+serverTarget);
		if (shortLink != null && farLink != null) {
			if (distance(serverTarget, shortLinkId) <= distance(serverTarget, farLinkId)) {
				System.out.println("[" + getSelf().path().name()
						+ "] Traversing ButterFly : Contacting ShortLink   ==>   " + shortLink.path().name());
				shortLink.tell(new Message.LookUp(ViceroySystem.valueString, "traverseButterFLy"), ActorRef.noSender());
			} else {
				System.out.println("[" + getSelf().path().name()
						+ "] Traversing ButterFly : Contacting FarLink     ==>   " + farLink.path().name());
				farLink.tell(new Message.LookUp(ViceroySystem.valueString, "traverseButterFly"), ActorRef.noSender());
			}
		} else if (shortLink != null) {
			System.out.println("[" + getSelf().path().name() + "] Traversing ButterFly : Contacting ShortLink   ==>   "
					+ shortLink.path().name());
			shortLink.tell(new Message.LookUp(ViceroySystem.valueString, "traverseButterFLy"), ActorRef.noSender());
		} else if (farLink != null) {
			System.out.println("[" + getSelf().path().name() + "] Traversing ButterFly : Contacting FarLink     ==>   "
					+ farLink.path().name());
			farLink.tell(new Message.LookUp(ViceroySystem.valueString, "traverseButterFly"), ActorRef.noSender());
		}
	}

	public boolean msgFound(String stringRecv) {
		// System.out.println("Looking Up data - " + stringRecv + " on " +
		// getSelf().path().name());
		// search the bucket for value
		for (String value : this.serverBucket) {
			if (value.equals(stringRecv)) {
				// set the lookup node to the current node
				ViceroySystem.SharedMem.lookupNode = this.sid;
				// update the node which has the value
				ViceroySystem.SharedMem.lookupDone = true;

				System.out.println();
				System.out.println("************** MESSAGE FOUND AT : " + getSelf().path().name() + " **************");
				System.out.println();

				return true;
			}
		}
		return false;
	}

	public void printLinks() {
		System.out.println("===For ServerNode [" + sid + "]====");
		if (pred != null)
			System.out.println("Predeccessor : " + pred.path().name());
		if (succ != null)
			System.out.println("Successor     : " + succ.path().name());
		if (farLink != null)
			System.out.println("farLink      : " + farLink.path().name());
		if (shortLink != null)
			System.out.println("shortLink    : " + shortLink.path().name());
		if (upLink != null)
			System.out.println("upLink       : " + upLink.path().name());
		System.out.println("================DONE================");
	}

	public void populateLinks() {
		int predOffset = 0;
		int succOffset = 0;
		int farLinkOffset = 0;
		int shortLinkOffset = 0;
		int upLinkOffset = 0;

		// System.out.println("populating links");

		level = sid % ViceroySystem.numLevels;
		posInLevel = sid / ViceroySystem.numLevels;

		pred = ViceroySystem.NodeRef[(level) - 1
				+ ((ViceroySystem.numLevels - (level)) / ViceroySystem.numLevels)
						* (ViceroySystem.numLevels)][(ViceroySystem.maxNodesAtLevel + posInLevel
								- ((ViceroySystem.numLevels - level) / ViceroySystem.numLevels))
								% (ViceroySystem.maxNodesAtLevel)];

		// System.out.println("pred first attempt done");

		while (pred == null && pred != getSelf() && predOffset < ViceroySystem.numNodes) {
			predOffset++;
			pred = ViceroySystem.NodeRef[(level - predOffset) - 1
					+ ((ViceroySystem.numLevels - (level - predOffset)) / ViceroySystem.numLevels)
							* (ViceroySystem.numLevels)][(ViceroySystem.maxNodesAtLevel + posInLevel
									- ((ViceroySystem.numLevels - (level - predOffset)) / ViceroySystem.numLevels))
									% (ViceroySystem.maxNodesAtLevel)];
		}

		if (pred == null || pred == getSelf() || predOffset >= ViceroySystem.numNodes) {
			// System.exit(0);
			// System.out.println("%%%%% I AM AN ISSUE @ PRED %%%%%%");
		}

		// System.out.println("pred decided");

		succ = ViceroySystem.NodeRef[(level + 1)
				% ViceroySystem.numLevels][(posInLevel + (level + 1) / ViceroySystem.numLevels)
						% (ViceroySystem.maxNodesAtLevel)];

		// System.out.println("succ first attempt done");

		while (succ == null && succ != getSelf() && succOffset < ViceroySystem.numNodes) {
			succOffset++;
			succ = ViceroySystem.NodeRef[(level + succOffset + 1)
					% ViceroySystem.numLevels][(posInLevel + (level + succOffset + 1) / ViceroySystem.numLevels)
							% (ViceroySystem.maxNodesAtLevel)];
		}

		if (succ == null || succ == getSelf() || succOffset >= ViceroySystem.numNodes) {
			// System.exit(0);
			System.out.println("%%%%% I AM AN ISSUE @ SUCC %%%%%%");
		}

		// System.out.println("succ decided");

		// System.out.println("Level : " + level + " max level for far link" +
		// (ViceroySystem.numLevels - 1));

		if (level == ViceroySystem.numLevels - 1) {
			shortLink = null;
			farLink = null;
			// System.out.println("farLink decided");
			// System.out.println("shortLink decided");
		} else {
			farLink = ViceroySystem.NodeRef[level + 1][((posInLevel + ViceroySystem.maxNodesAtLevel / (2 * (level + 1)))
					% (ViceroySystem.maxNodesAtLevel / (level + 1))
					+ ((ViceroySystem.maxNodesAtLevel / (level + 1))
							* (posInLevel / (ViceroySystem.maxNodesAtLevel / (level + 1)))))
					% ViceroySystem.maxNodesAtLevel];

			// System.out.println("farlink first attempt done");

			while (farLink == null && farLinkOffset < ViceroySystem.maxNodesAtLevel
					&& (level + 1 + farLinkOffset) < ViceroySystem.numLevels - 1) {
				farLink = ViceroySystem.NodeRef[level + 1][((posInLevel + farLinkOffset
						+ ViceroySystem.maxNodesAtLevel / (2 * (level + 1)))
						% (ViceroySystem.maxNodesAtLevel / (level + 1))
						+ ((ViceroySystem.maxNodesAtLevel / (level + 1))
								* ((posInLevel + farLinkOffset) / (ViceroySystem.maxNodesAtLevel / (level + 1)))))
						% ViceroySystem.maxNodesAtLevel];
				farLinkOffset++;
			}

			// System.out.println("farLink decided");

			shortLink = ViceroySystem.NodeRef[level + 1][posInLevel];

			// System.out.println("shortlink first attempt done");

			while (shortLink == null && (level + 1 + shortLinkOffset) < ViceroySystem.numLevels - 1) {
				shortLink = ViceroySystem.NodeRef[level + 1 + shortLinkOffset][posInLevel];
				shortLinkOffset++;
			}

			// System.out.println("shortLink decided");

		}

		if (level == 0) {
			upLink = null;
			// System.out.println("upLink decided");

		} else {
			upLink = ViceroySystem.NodeRef[level - 1][posInLevel];

			// System.out.println("uplink first attempt done");

			while (upLink == null && (level - upLinkOffset) > 0) {
				upLink = ViceroySystem.NodeRef[level - 1 - upLinkOffset][posInLevel];
				upLinkOffset++;
			}

			// System.out.println("upLink decided");

		}

		// printLinks once populated
		// printLinks();
	}

	public ServerNode(int sid, int level, int posInLevel, ActorRef pred, ActorRef succ, ActorRef shortLink,
			ActorRef farLink, ActorRef upLink) {
		this.sid = sid;
		this.level = level;
		this.posInLevel = posInLevel;
		this.phase = null;
		this.succ = succ;
		this.pred = pred;
		this.shortLink = shortLink;
		this.farLink = farLink;
		this.upLink = upLink;
	}

	public void onReceive(Object message) throws Exception {

		if (message instanceof Message.Join) {

			// System.out.println(getSelf().path().name() + " : Server Node Joined");

			// communicate with the next successor node to get values belonging
			// to current node
			Message.CollectData collectData = new Message.CollectData();

			// get the links for the current joining node
			populateLinks();

			// System.out.println("collecting data from successor : " + succ.path().name());

			// send the collect data message to the next node
			succ.tell(collectData, getSelf());

		} else if (message instanceof Message.Leave) {

			// System.out.println(getSelf().path().name() + " : Server Node
			// Killed");

			// get the links for the current leaving node
			populateLinks();

			// transfer the current server bucket to the next node
			Message.DistributeData serverBucket = new Message.DistributeData(this.serverBucket);

			succ.tell(serverBucket, getSelf());

			ViceroySystem.participatingNodesCount--;

			// kill the node
			ViceroySystem.NodeRef[level][posInLevel] = null;
			ViceroySystem.SharedMem.updateDone = true;
			getContext().stop(getSelf());

		} else if (message instanceof Message.LookUp) {
			
			ViceroySystem.lookUpCount++;

			// store the data for the server in the current server bucket
			if (!msgFound(((Message.LookUp) message).valueString)) {
				// populate links before lookup
				populateLinks();

				// get the current phase
				phase = ((Message.LookUp) message).phase;

				// come up with the strategy to move close to the node where the
				// data might be stored
				int serverTarget = Hashing.consistentHash(
						Hashing.md5().hashString((((Message.LookUp) message).valueString), charsetFMT),
						ViceroySystem.numNodes);
				// System.out.println("ServerTarget : "+ serverTarget);

				// need to move close to server target
				if (moveToTopLevel(phase)) {
					// if at the top level, decide between farlink and shortlink
					if ((farLink != null || shortLink != null) && !(phase.equals("traverseRing")
							|| phase.equals("traversePred") || phase.equals("traverseSucc"))) {
						traverseButterFly(serverTarget);
					} else if (phase.equals("traversePred") || phase.equals("traverseSucc")) {
						traverseRing(serverTarget, phase);
					} else {
						phase = "traverseRing";
						traverseRing(serverTarget, phase);
					}
				}
			}

		} else if (message instanceof Message.DistributeData) {

			// append the temp server bucket to the current server bucket
			for (String s : ((Message.DistributeData) message).serverBucket) {
				serverBucket.add(s);
			}

			ViceroySystem.SharedMem.updateDone = true;

		} else if (message instanceof Message.CollectData) {

			// server Bucket to transfer to the previous live node
			ArrayList<String> serverValuesPrev = new ArrayList<String>();
			ArrayList<String> serverValuesCurr = new ArrayList<String>();

			// System.out.println(getSelf().path().name() + " Server started transferring data back to the previous node");

			// consistent hash to populate the new server bucket
			for (String s : serverBucket) {
				int bucket = Hashing.consistentHash(Hashing.md5().hashString(s, charsetFMT), 2);
				if (bucket == 0) {
					serverValuesCurr.add(s);
				} else {
					serverValuesPrev.add(s);
				}
			}

			// update the current node bucket
			serverBucket = serverValuesCurr;

			// transfer the current server bucket to the next node
			Message.DistributeData serverBucket = new Message.DistributeData(serverValuesPrev);

			// System.out.println("Sending data back to : " + getSender().path().name());

			// transfer the new server bucket for the joining node
			getSender().tell(serverBucket, getSelf());

		} else {
			unhandled(message);
		}

	}
}
