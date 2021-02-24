package hdfs.DataNode;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import formats.Format;
import formats.KV;
import hdfs.Commande;
import hdfs.HdfsServer;
import hdfs.Message;

/* Cette classe sera utilisé par HdfsClient afin de gérer les tmps des nodes */
public class ManipulationData {

    /* récuperer les fragments associés à un nom de fichier */
	/* nbNodes = Le nombre de dataNode
	 * nodesIP = La liste des IP des ordinateur sur lesquel tournent les HDFSServer (i.e. des datanodes)
	 * nodesPorts = la liste des ports des HdfsServer
	 * hdfsFname = Le nom du fichier a récupérer*/
    public static List<Data> getFragments(int nbNodes, ArrayList<String> nodesIP, ArrayList<Integer> nodesPorts, String hdfsFname) {
    	List<Data> lFrag = new ArrayList<Data>();
    	
    	//Récup des fragments sur chaque datanodes
    	for(int i=0; i < nbNodes; i++) {
    		Data f = getFragment(hdfsFname, nodesIP.get(i), nodesPorts.get(i));
    		if(f != null) {
    			lFrag.add(f);
    		}else {
    			System.out.print("ManipulationData::getFragments dit Fragment perdu en route...");
    		}
    	}
    	
    	
		return lFrag;
	}

    /* récuperer un fragment donné */
    private static Data getFragment(String hdfsFname, String dataNodeIP, int dataNodePort) {
    	Data fragment = null;
    	try {
    		//Socket étant connecté au serveur
    		Socket serv = new Socket(dataNodeIP, dataNodePort);
    		
    		//Envoie demande de Read
    		Message msg = new Message(Commande.CMD_READ, hdfsFname, 0);
    		OutputStream os = serv.getOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(os);
            oos.writeObject(msg);
            
            //Récup fragment
            InputStream is = serv.getInputStream();
            ObjectInputStream ois = new ObjectInputStream(is);
            fragment = (Data) ois.readObject();
    		
    	}catch(IOException e) {e.printStackTrace();}
    	catch(ClassNotFoundException e){e.printStackTrace();}
    	
    	
		return fragment;
	}

}
