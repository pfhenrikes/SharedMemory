package Client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import compute.Example;

public class Client {

	public static void main(String[] args) {
		Example example;
		
		try {
			Registry registry = LocateRegistry.getRegistry("localhost", 30000);
			example = (Example) registry.lookup("Example");
			System.out.println("> " + example.sayHello());
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
