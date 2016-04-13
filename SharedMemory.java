package project.systems.distributed.jhalakpatel;

public class SharedMemory {
	public boolean updateDone;
	public boolean lookupDone;
	public int lookupNode;

	public SharedMemory() {
		lookupNode = -1;
		lookupDone = false;
		updateDone = false;
	}
}