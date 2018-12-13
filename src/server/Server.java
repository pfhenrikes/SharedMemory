package server;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import compute.Example;

public class Server {
	
	public static void main(String[] args) {
			
		try {
			Example impl = new ExampleImpl();
			Example stub = (Example) UnicastRemoteObject.exportObject(impl, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind("Example", stub);
			System.out.println("Server ready...");
		} catch (RemoteException e) {
			System.out.println("ERROR");
			e.printStackTrace();
		}
		
	}
	
}
