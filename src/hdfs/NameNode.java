package hdfs;
import java.io.*;
import java.net.*;
import java.util.HashMap;

import ordo.Worker;

public class NameNode extends Remot{
		private final static String nomFichierHdfs;
		private final static int nbreNode;

		/*Liste des noeuds qui contiennent les fragments associés à un fichier sur HDFS avec la condition de nommage définie*/
		private final static  List<String> listeNode;

	   public static void main(String args[]) {

		try {
			Socket client = new Socket("localhost", 2983);
			ObjectInputStream ois = null;
			ObjectOutputStream oos = null;
			Scanner input = new Scanner(System.in);
	
			for (int i = 0; i <= nbreNode;) {
				oos.writeObject(nomFichierHdfs + String.valueOf(i));
				// envoi
				System.out.print("Le client demande le fragment: nomFichierHdfs" + String.valueOf(i)"\t");
				oos = new ObjectOutputStream(client.getOutputStream());
				oos.write(nomFichierHdfs + String.valueOf(i));
				oos.close();
	
				// reçu
				ois = new ObjectInputStream(client.getInputStream());
				String msg = ois.read();
				System.out.println(msg);
				String[] flag = msg.split(":");
				if(flag[0].equals("ACK")){
					i++;
				}else{
					i--;
				}
			}
			System.out.println("By to server");
	
			ois.close();
			oos.close();
			client.close();
		} catch (Exception ex) {
			System.out.println(ex);
		}
	}

	/** Renvoie la liste des fragments associés à un fichier sur HDFS avec la condition de nommage des fragments est la suivante : nomFichier + i où i est le numéro du fragment
	*/
	public List<String> getFragments(String nomFichier) throws RemoteException;

	/** Indique si les fragments du fichier existe ou non à partir de son nom */
	public boolean siExiste(String name) throws RemoteException;
}