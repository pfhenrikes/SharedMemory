package jms;

import java.io.Serializable;

public class SharedVariable implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int number;
	private int id;
	
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public SharedVariable(int number, int id) {
		super();
		this.number = number;
		this.id = id;
	}
	
	
}
