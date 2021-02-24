package hdfs;
import java.io.*;
import java.net.*;
import java.util.ArrayList;

import formats.Format;
import formats.KV;
import formats.KVFormatS;
import formats.LineFormatS;
import hdfs.DataNode.Data;

public class HdfsServer {
	public static final int DEFAULT_PORT = 1234;// Port par défaut si aucun renseigné
	private static String repertory = "Hidoop_Rep";
	
	private ServerSocket serv;
	private Socket client;
	private int port;//Le port effectif d'attente du server
	
	public HdfsServer(int p) {
		this.port = p;
		//Creating the repertory for file system
		File rep = new File(repertory);
		rep.mkdir();
		try {
			//creation du socket représentant le HDFSServer
			serv = new ServerSocket(p);
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public HdfsServer() {
		this(HdfsServer.DEFAULT_PORT);
	}
	
	public void waitConnection() {
		try {
			//Attente de connection de la part du client
			client = serv.accept();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	//Reception d'une instance de l'objet NetworkMessage via le socket du client.
	public Message receiveMessage() {
		Message msgReceived = null;
		try{
			InputStream is = client.getInputStream();
			ObjectInputStream ois = new ObjectInputStream(is);
			
			msgReceived = (Message) ois.readObject();
			System.out.println("-----Received message content-----");
			System.out.print(msgReceived);
		}
		catch(IOException e) {e.printStackTrace();}
		catch(ClassNotFoundException e) {System.out.print("Object of type Message not received"); e.printStackTrace();}
		return msgReceived;
	}
	
	public Data receiveData() {
		Data dataReceived = null;
		try{
			InputStream is = client.getInputStream();
			ObjectInputStream ois = new ObjectInputStream(is);
			
			dataReceived = (Data) ois.readObject();
			System.out.println("-----Received message content-----");
			System.out.print(dataReceived);
		}
		catch(IOException e) {e.printStackTrace();}
		catch(ClassNotFoundException e) {System.out.print("Object of type Data not received"); e.printStackTrace();}
		return dataReceived;
	}
	
	public void executeRequest(Message msg, Data inputData) {
		String filePath = HdfsServer.repertory + "/" + msg.getNom();
		switch(msg.getCommande()){
			//Lire les données d'un fichier au format KV (traité par map)
			case CMD_READ:
				String processedFilePath = HdfsServer.repertory + "/" + msg.getNom() + "_res";
				System.out.println("Executing Read command on " + processedFilePath);
				System.out.println("Reading file...");
				
				//Ouverture fichier
				KVFormatS kvfile = new KVFormatS(processedFilePath);
				kvfile.open(Format.OpenMode.R);
				
				//Lecture du fichier
				ArrayList<KV> data = new ArrayList<KV>();
				KV kv = kvfile.read();
				while(kv != null) {
					data.add(kv);
				}
				System.out.println("Reading Done");
				
				//ENVOIE DU MESSAGE AU CLENT
				Data dataToSend = new Data(data);
				try{
					OutputStream os = client.getOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(os);
					oos.writeObject(dataToSend);
					System.out.println("-----Message Sent-----");
					System.out.print(dataToSend);
					os.close();
					oos.close();
				}
				catch(IOException e) {e.printStackTrace();}
			break;
			
			//Ecrire les données recus par TCP au format LINE
			case CMD_WRITE:
				System.out.println("Executing Write command on " + filePath);
				System.out.println("Writing to file...");
				//Ecriture dans le fichier
				LineFormatS lfile = new LineFormatS(filePath);
				lfile.open(Format.OpenMode.W);
				for(int i=0; i < inputData.getLength(); i++) {
					lfile.write(inputData.get(i));
				}
				System.out.println("Writing Done");
			break;
			case CMD_DELETE:
				System.out.println("Executing Delete command");
				//Pour supprimer un fichier on passe pas File car *FormatS ne le permet pas
				try {
					File fileDel = new File(filePath);
					fileDel.delete();
					System.out.println("Deletion Done");
				}catch(SecurityException e) {
					e.printStackTrace();
				}
			break;
		}
		return;
	}
	
	public void close() {
		try {
			serv.close();
			client.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getClientInfo() {
		return client.getInetAddress().getHostAddress() + ":" + String.valueOf(client.getPort());
	}
	
	public int getPort() {return this.port;}
	
	public static void main(String[] args) {
		HdfsServer s = null;
		if(args.length > 1)
			s = new HdfsServer(Integer.parseInt(args[0]));
		else
			s = new HdfsServer();//Port par défaut 1234
		
		System.out.print("Server waiting for a connection on port " + s.getPort() + " ...");
		s.waitConnection();
		System.out.print("Connected to " + s.getClientInfo());
		
		//Reception des données de la part du HdfsClient
		Message m = s.receiveMessage();
		Data d = s.receiveData();

		//Execution de la requete du client
		System.out.print("Executing request...");
		s.executeRequest(m, d);//Execute selon les données de msgReceived
		
	}

}
