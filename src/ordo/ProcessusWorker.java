package ordo;

import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;

public class ProcessusWorker implements Runnable {

	private Worker wr;
	private Mapper m; 
	private Format reader;
	private Format writer;
	private CallBack cb;
	
	
	
	public ProcessusWorker(Worker wr, Mapper m, Format reader, Format writer, CallBack cb) {
		super();
		this.wr = wr;
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}


	@Override
	public void run() {
		try {
			System.out.println("thread worker lancé");
			wr.runMap(m, reader, writer, cb);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
}
