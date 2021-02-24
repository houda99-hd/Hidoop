/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.DataNode.Data;

import static hdfs.Commande.CMD_WRITE;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.ArrayList;
import java.util.List;

public class HdfsClient  {
	// les servers utilisés
    final static int[] ports = {};
    final static int nombrePorts = ports.length;
	static String[] host;
    static Socket clientSocket;
    static Commande commande;

    private static void usage() {
    	
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
        for (int i = 0; i<= nombrePorts; i++) {
            //établir une connection avec le serveur i 
            try {
				clientSocket = new Socket(host[i],ports[i]);
	            //récupérer le outputObject pour envoyer le message
	            OutputStream output = clientSocket.getOutputStream();
	            ObjectOutputStream io = new ObjectOutputStream(output);
	            //définir le message à envoyer
	            String name = String.valueOf(i);
	            Message message = new Message(commande.CMD_DELETE, hdfsFname + name , hdfsFname.length());
	            io.writeObject(message);
	            output.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) throws IOException { 
	//repFactor est le facteur de duplication des fragments = 1 (pas de duplication).
		Data data = null;
		String tmp = new String();
		Format f = null;
        //écrire les fragments selon le format
        if (fmt == Format.Type.LINE ) {
        	f = new LineFormat(localFSSourceFname);
		} else if (fmt == Format.Type.KV) {
				f = new KVFormat(localFSSourceFname);
		}
            f.open(Format.OpenMode.R);
            /* calculer nombre de fragments nécessaires */
            BufferedReader copieBuffer = null;
			try {
				copieBuffer = new BufferedReader(new FileReader(new File(localFSSourceFname)));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			};
            //récupérer le nombre de lignes du fichier
            int nbLigne = 0;
			while ( copieBuffer.readLine() != null) {
                nbLigne++;
            }
	        copieBuffer.close();
            int nbLigneFragments = Math.round(nbLigne/nombrePorts);

	    /* écriture des fragments */
	     KV lecture;
             for (int i= 0; i<= nbLigneFragments; i++) {
			    lecture = f.read();
			     data.add(lecture);
             }
             f.close();

	     /* configurer le message */
             for (int i = 0; i<= nombrePorts; i++) {
                     //établir une connection avec le serveur i 
                     try {
						clientSocket = new Socket(host[i],ports[i]);
	                     //récupérer le outputObject pour envoyer le message
	                     OutputStream output = clientSocket.getOutputStream();
	                     //définir le message à envoyer
	                     ObjectOutputStream io = new ObjectOutputStream(output);
	                     //définir le message à envoyer
	                     String name = String.valueOf(i);
	                     Message message = new Message(commande.CMD_WRITE,localFSSourceFname + name,localFSSourceFname.length());
	                     io.writeObject(message);
	    		    	 KV d = data.get(i);
	                     io.writeObject(d);
	                     output.close();
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}   
             }
	}

    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws ClassNotFoundException {
	//récupérer une liste de fragments associés au fichier hdfsFname
	Format fw = null;
		//Dans le cas de KVList
        Format fw1  = new KVFormat(localFSDestFname);
        //FileWriter fw = new FileWriter(file, true);
	//BufferedWriter associé à cette format
	fw1.open(Format.OpenMode.W);
	for (int i = 0; i<= nombrePorts; i++) {
                        //établir une connection avec le serveur i 
                        try {
							clientSocket = new Socket(host[i],ports[i]);
	                        //récupérer le outputObject pour envoyer le message
	                        OutputStream output = clientSocket.getOutputStream();
	                        //définir le message à envoyer
	                        String name = String.valueOf(i);
		                     //définir le message à envoyer
		                    ObjectOutputStream io = new ObjectOutputStream(output);
	                        Message message = new Message(commande.CMD_READ, hdfsFname + name , 0);
	                        io.writeObject(message);
	                        InputStream input = clientSocket.getInputStream();
	                        ObjectInputStream is = new ObjectInputStream(input);
	                        Data d = (Data) is.readObject();
	                        int k = 0;
	                        KV kv = d.get(k);
	                        while (kv != null) {
	                        	fw1.write(kv);
	                        	k++;
	                        }
	                        output.close();
	                        input.close();
	                        fw1.close();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {							// TODO Auto-generated catch block
							e.printStackTrace();
						}
            }
    }   
public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {
            	usage(); return;
            }
            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}

