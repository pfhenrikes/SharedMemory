package server;

import java.rmi.RemoteException;

import compute.Example;

public class ExampleImpl implements Example {

	@Override
	public String sayHello() throws RemoteException {
		return "Hello!";
	}

}
