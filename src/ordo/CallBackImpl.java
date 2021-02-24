package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
    private ReentrantLock moniteur;
    private Condition waitFinished;
    private Condition finished;

	private static int nbMapEnCours;
	
	public CallBackImpl(int nbWorker) throws RemoteException {
        super();
    	this.moniteur = new ReentrantLock();
        this.finished = moniteur.newCondition();
        nbMapEnCours = nbWorker;
    }
	
	

	@Override
	public void println(String s) throws RemoteException{
		 System.out.println(s);		 
	}
	
	@Override
	public void onFinishedMap() throws RemoteException {
    	moniteur.lock();
    	while(nbMapEnCours != 0) {
	    	try {
				finished.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.out.println("HEEEERE");
	
			}	
    	}
		System.out.println("tous les maps ont finis");

    	moniteur.unlock();
	}
	
	@Override
	public void call() throws RemoteException{
    	moniteur.lock();
		nbMapEnCours--;
		System.out.println("map fini");
		if (nbMapEnCours == 0) {
			System.out.println("signaled!");
			finished.signal();
		}
    	moniteur.unlock();
	}

}
