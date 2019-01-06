package jms;

import javax.jms.JMSException;

public interface NodeInterface {
	// writeMemory()
	// readMemory()
	
	public void login(int id);
		
	public void logout();
	
	public SharedVariable read();
	
	public void write(int value) throws JMSException;
	
}
