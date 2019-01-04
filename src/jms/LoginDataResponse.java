package jms;

import java.io.Serializable;

public class LoginDataResponse implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int nextNode = 0;
	private int leaderId = 0;
	
	public LoginDataResponse(int nextNode, int leaderId) {
		super();
		this.nextNode = nextNode;
		this.leaderId = leaderId;
	}

	public int getNextNode() {
		return nextNode;
	}

	public void setNextNode(int nextNode) {
		this.nextNode = nextNode;
	}

	public int getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(int leaderId) {
		this.leaderId = leaderId;
	}

}
