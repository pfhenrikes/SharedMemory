package compute;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Example extends Remote {
	public String sayHello() throws RemoteException;
}
