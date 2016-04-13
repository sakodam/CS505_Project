package project.systems.distributed.jhalakpatel;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.AskableActorSelection;
import akka.util.Timeout;
import scala.PartialFunction;
import scala.concurrent.Await;
import scala.concurrent.Awaitable;
import scala.concurrent.Promise;
import scala.concurrent.duration.Duration;
import scala.concurrent.impl.Future;
import scala.runtime.BoxedUnit;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.hash.Hashing;

public class ServerNode extends UntypedActor {

	int sid;
	int token;
	ActorRef pred;
	ActorRef succ;
	// private static ArrayList<String> serverBucket;
	ArrayList<String> serverBucket = new ArrayList<String>();

	public ServerNode() {
		sid = 0;
		token = 0;
		pred = null;
		succ = null;
	}

	public ServerNode(int sid, int token) {
		this.sid = sid;
		this.token = token;
		pred = null;
		succ = null;
	}

	public ServerNode(int sid, int token, ActorRef pred, ActorRef succ) {
		this.sid = sid;
		this.token = token;
		this.pred = pred;
		this.succ = succ;
	}

	public void onReceive(Object message) throws Exception {

		if (message instanceof Message.Join) {

			this.pred = ((Message.Join) message).pred;
			this.succ = ((Message.Join) message).succ;

			System.out.println(getSelf().path().name() + " : Server Node Joined");

			// communicate with the next node to get values belonging to current node
			Message.CollectData collectData = new Message.CollectData();

			// send the stop message to the next node
			this.succ.tell(collectData, getSelf());

		} else if (message instanceof Message.Leave) {

			this.pred = ((Message.Leave) message).pred;
			this.succ = ((Message.Leave) message).succ;

			System.out.println(getSelf().path().name() + " : Server Node Killed");

			// transfer the current server bucket to the next node
			Message.DistributeData serverBucket = new Message.DistributeData(this.serverBucket);

			// send the stop message to the next node
			this.succ.tell(serverBucket, getSelf());

			RoundRobinCommSystem.SharedMem.updateDone = true;

			// kill the node
			getContext().stop(getSelf());

		} else if (message instanceof Message.LookUp) {

			String stringRecv;
			// store the data for the server in the current server bucket
			stringRecv = ((Message.LookUp) message).valueString;

			//System.out.println("Looking Up data - " + stringRecv + " on " + getSelf().path().name());

			boolean found = false;
			// search the bucket for value
			for (String value : this.serverBucket) {
				if (value.equals(stringRecv)) {
					found = true;
				}
			}

			if (found == true) {

				// set the lookup node to the current node
				RoundRobinCommSystem.SharedMem.lookupNode = this.sid;
				
				// update the node which has the value
				RoundRobinCommSystem.SharedMem.lookupDone = true;

				// System.out.println(getSelf().path().name() + " VALUE
				// FOUND!!!");

			} else {

				// now need to search on the successor nodes
				this.pred = ((Message.LookUp) message).pred;
				this.succ = ((Message.LookUp) message).succ;
				int succnextid = 0;
				int succid = this.sid + 1;

				boolean succAct = getContext().actorFor(this.succ.path()).isTerminated();

				// iterate till we get a live actor in clockwise direction
				while (succAct == true) {

					// System.out.println("found node is dead");
					// get a new successor
					this.succ = RoundRobinCommSystem.NodeRef[succid + 1];
					succAct = getContext().system().actorFor(this.succ.path()).isTerminated();

					// increase succ id for the next round
					if (succid == RoundRobinCommSystem.numNodes - 1) {
						succid = 0;
					} else {
						succid++;
					}
				}

				// account for the last extra increase
				succid--;
				
				if (succid == RoundRobinCommSystem.numNodes-1) {
					succnextid = 0;
				} else {
					succnextid = succid + 1;
				}

				// prepare communication message
				Message.LookUp LookUp = new Message.LookUp(stringRecv, getSelf(),
						RoundRobinCommSystem.NodeRef[succnextid]);

				// send the stop message to the next node
				this.succ.tell(LookUp, getSelf());
			}
		} else if (message instanceof Message.DistributeData) {

			// append the temp server bucket to the current server bucket
			for (String s : ((Message.DistributeData) message).serverBucket) {
				this.serverBucket.add(s);
			}

		} else if (message instanceof Message.CollectData) {

			// server Bucket to transfer to the previous live node
			ArrayList<String> serverValuesPrev = new ArrayList<String>();
			ArrayList<String> serverValuesCurr = new ArrayList<String>();
			
			System.out.println(getSelf().path().name()+" Server started transferring data back to the previous node");

			Charset charsetFMT = Charset.forName("UTF-8");

			// consistent hash to populate the new server bucket
			for (String s : this.serverBucket) {
				int bucket = Hashing.consistentHash(Hashing.md5().hashString(s, charsetFMT), 2);
				if (bucket == 0) {
					serverValuesPrev.add(s);
				} else {
					serverValuesCurr.add(s);
				}
			}

			// update the current node bucket
			this.serverBucket = serverValuesCurr;

			// transfer the current server bucket to the next node
			Message.DistributeData serverBucket = new Message.DistributeData(serverValuesPrev);

			// transfer the new server bucket for the joining node
			getSender().tell(serverBucket, getSelf());

		} else {

			unhandled(message);

		}

	}
}
