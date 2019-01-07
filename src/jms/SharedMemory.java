package jms;

import java.util.Map;
import java.util.TreeMap;

public class SharedMemory {
	
	// Map address to variable
	// example of memory
	private Map<Integer, SharedVariable> memory = new TreeMap<Integer, SharedVariable>();
	
	public SharedMemory() {
		memory.put(439041089, new SharedVariable(0,0));
		memory.put(439041090, new SharedVariable(10,0));
		memory.put(439041091, new SharedVariable(20,0));
		memory.put(439041092, new SharedVariable(30,0));
	}
	
	public SharedVariable getVariable(int address) {
		return memory.get(address);
	}
	
	public void setSharedVariable(int address, int value, int id) {
		SharedVariable temp = memory.get(address);
		temp.setNumber(value);
		temp.setId(id);
		memory.put(address, temp);
	}	
	
}
