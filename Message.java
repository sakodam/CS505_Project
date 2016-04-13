package project.systems.distributed.jhalakpatel;

import java.io.Serializable;
import java.util.ArrayList;

import akka.actor.ActorRef;

public class Message {
	/* type of message needs to be sent */

	public static class Join implements Serializable {
		public ActorRef pred;
		public ActorRef succ;

		public Join(ActorRef pred, ActorRef succ) {
			this.pred = pred;
			this.succ = succ;
		}
	}

	public static class Leave implements Serializable {
		public ActorRef pred;
		public ActorRef succ;

		public Leave(ActorRef pred, ActorRef succ) {
			this.pred = pred;
			this.succ = succ;
		}
	}

	public static class LookUp implements Serializable {
		public String valueString;
		public ActorRef pred;
		public ActorRef succ;

		public LookUp(String valueString, ActorRef pred, ActorRef succ) {
			this.valueString = valueString;
			this.pred = pred;
			this.succ = succ;
		}
	}

	public static class DistributeData implements Serializable {
		public ArrayList<String> serverBucket;

		public DistributeData(ArrayList<String> serverBucket) {
			this.serverBucket = serverBucket;
		}
	}

	public static class CollectData implements Serializable {
	}

}
