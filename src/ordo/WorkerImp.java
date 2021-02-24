package ordo;

import java.rmi.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import formats.KV;
import formats.LineFormatS;
import map.Mapper;

public class WorkerImp extends UnicastRemoteObject implements Worker {

	private String nom;
	private int port;
	
	protected WorkerImp(String nom, int port) throws RemoteException {
		super();
		this.nom = nom;
		this.port = port;
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) {
		
		reader.open(Format.OpenMode.R);
		writer.open(Format.OpenMode.W);
		
		System.out.println("Execute Mapper");
		m.map(reader, writer);
		
		reader.close();
		writer.close();

		
		/* Appeler le callback pour dire que le map a fini */
		try {
			cb.call();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	

	
	public static void main(String[] args) {
		try {
			Registry registry = LocateRegistry.createRegistry(Integer.parseInt(args[1]));
			WorkerImp worker = new WorkerImp(args[0],Integer.parseInt(args[1]));
			System.out.println("Rebind on : //localhost:" + worker.getPort() + "/" + worker.getNom());
			Naming.rebind("//localhost:" + worker.getPort() + "/" + worker.getNom() ,worker);
			System.out.println("WorkerImp" + worker.getNom() + " bound in registry");
			
			
			LineFormatS lfile = new LineFormatS("FichierTest" + args[0]);
			lfile.open(Format.OpenMode.W);
			for(int i=0; i < 5; i++) {
				lfile.write(new KV(String.valueOf(i), "superTest" + i));
			}
			lfile.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getNom() {
		return nom;
	}

	public void setNom(String nom) {
		this.nom = nom;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
