package project.systems.distributed.jhalakpatel;

import java.io.Serializable;
import java.util.ArrayList;

import akka.actor.ActorRef;

public class Message {
	/* type of message needs to be sent */
	public static class Join implements Serializable {
		public Join() {
		}
	}

	public static class Leave implements Serializable {
		public Leave() {
		}
	}

	public static class LookUp implements Serializable {
		public String valueString;
		public String phase;
		
		public LookUp(String valueString, String phase) {
			this.valueString = valueString;
			this.phase = phase;
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
