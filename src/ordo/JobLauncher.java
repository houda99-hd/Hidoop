package ordo;

import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import application.MyMapReduce;
import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import formats.KVFormatS;
import formats.LineFormat;
import formats.LineFormatS;
import map.MapReduce;
import map.Mapper;

public class JobLauncher implements JobInterface {
	
	private Type inputFormat = Format.Type.KV;    /* Format du fichier en entrée (LINE/KV) */
	private String inputFname;	 /* Nom du fichier HDFS contenant les données à traiter */
	private Type outputFormat;   /* Format du fichier en sortie (LINE/KV) */
	private String outputFname;	 /* Nom du fichier résultat après le reduce */
	
	/* Liste contenant les noms des machines distantes et les ports qu'elles utilisent (cf. 2.1 du sujet) */
    HashMap<String,Integer> hm = new HashMap<String,Integer>();
    
    public JobLauncher(HashMap<String,Integer> hm) {
    	this.hm = hm;
    }
	
	@Override
	public void startJob(MapReduce mr) {
		
		Format input, tmp, output;
		switch (inputFormat) {
		case LINE :
			input = new LineFormatS(inputFname);
			break;
		case KV : 
			input = new LineFormatS(inputFname);
			break;
		default : 
			System.out.println("format de fichier indisponible.");
		}        
        /**/
        tmp = new KVFormatS(inputFname+"_tmp");
        
		output = new KVFormatS(inputFname+"-res");
		
		/* Fragmenter le fichier input avec HDFSWrite */
		
		/* Initialiser le callback pour le retour des maps */
		CallBack cb = null;
		try {
			cb = new CallBackImpl(this.hm.size());
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		
		/* Parcourir la liste des workers */
		for (Entry<String, Integer> entry : hm.entrySet()) {
		    String nom = entry.getKey();
		    Integer port = entry.getValue();
		    
		/* contacter un deamon (worker) */
		    
		    /* rechercher l'objet dans l'annuaire */
			try {
				/* les workers sont créés ici car on ne récupère pas encore la liste des workers dans le NameNode 
				 * A modifier par la suite */
				Worker wr = (Worker) Naming.lookup("//localhost:" + port + "/" + nom);
			          
				/* créer les reader et writer des workers en fonction du format de fichier souhaité */
				Format reader, writer;
				reader = null;
				switch (inputFormat) {
					case LINE :
						reader = new LineFormatS(inputFname + "_" + nom);
						break;
					case KV : 
						reader = new KVFormatS(inputFname+ "_" + nom);
						break;
					default : 
						System.out.println("format de fichier indisponible.");
				}
				writer = new KVFormatS(inputFname+ "_" + nom+"_res");
			
				/* Créer le thread d'exécution du worker */
			    Runnable workerProc = new ProcessusWorker(wr,(Mapper)mr, reader, writer, cb);
	            Thread t = new Thread(workerProc);
	            t.start();
			
			/* lancer le runMap dans le node distant */
			//wr.runMap((Mapper)mr, reader, writer, cb);
			
			
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		/* Attendre la fin de tous les maps */
		try {
			System.out.println("J'attends la terminaison des maps ..");
			cb.onFinishedMap();
			System.out.println("Maps terminés! on reprend le boulot");
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		/* Rassembler les fichiers résultats des maps sur chaque machine avec HDFSRead dans le tmp et donner le fichier au reduce*/
		/* On lit sur le fichier résultat des différents map ie du HDFSRead */
		tmp.open(Format.OpenMode.R);
		
		/* On écrit sur le fichier résultat du reduce*/
		output.open(Format.OpenMode.W);
		
		/* On applique le reduce*/
		System.out.println("Execute Reducer");
		mr.reduce(tmp, output);
		
		output.close();
		tmp.close();

		

		// boucle qui fera des appels au worker sur tous les noeuds
			// contacter un deamon (worker) **RMI (contient par def un deamon) 
			// le worker appelle les map avec un reader et un writer et un callback bien choisi
			// lancer les map
		// recupérer les resultats (rassemple le fichier tmp généré par les maps) et faire le reduce

	}
	
	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}
	
	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
	}
	
	
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
	}
	
	
	public void setOutputFname(String fname) {
		this.outputFname = fname + "-res";
	}
	
}
