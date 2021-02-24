package ordo;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote {
	public void println(String s) throws RemoteException;
	public void onFinishedMap() throws RemoteException;
	public void call() throws RemoteException;
}
