package hdfs;
import java.io.Serializable;
import java.util.ArrayList;
import formats.KV;

public class NetworkMessage implements Serializable{
	private Commande cmd;
	private String fileName;
	private ArrayList<KV> data;

	public NetworkMessage() {
		data = new ArrayList<KV>();
		fileName = "";
		cmd = Commande.CMD_READ;
	}
		
	public Commande getCmd() {return this.cmd;}
	
	public String getFilename() {return this.fileName;}
	
	public ArrayList<KV> getData() {return this.data;}
	
	public int getDataLineNbr() {return data.size();}
	
	public KV getLine(int i) {return data.get(i);}
	
	public void setCmd(Commande c) {this.cmd = c;}
	
	public void setFilename(String n) {this.fileName = n;}
	
	public void setData(ArrayList<KV> d) {this.data = d;}
	
	public void addData(KV kv) {this.data.add(kv);}
	
	public String toString() {
		String dataBegin="";
		for(int i = 0; i < 5; i++) {
			dataBegin += data.get(i) + "\n";
		}
		return "Command = " + this.cmd + "\nFile name = " + this.fileName + "\nFirst lines = " + dataBegin;
	}
}
