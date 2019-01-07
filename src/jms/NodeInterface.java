package jms;

import javax.jms.JMSException;

public interface NodeInterface {
	// writeMemory()
	// readMemory()
	
	public void login(int id);
		
	public void logout();
	
	public SharedVariable read(int address);

	void write(int value, int address) throws JMSException;
	
}
