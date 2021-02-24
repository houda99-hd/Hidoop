package hdfs;

import java.io.Serializable;
import java.util.ArrayList;

import formats.Format;
import formats.KV;

public class Data implements Serializable {
	private ArrayList<KV> data;
	
	public Data(ArrayList<KV> d) {
		this.data = d;
	}
	
	public int getLength() {
		return data.size();
	}

	public KV get(int i) {
		return this.data.get(i);
	}
	
	public void add(KV val) {
		this.data.add(val);
	}
	
	//Affichage des 10 premières valeures
	public String toString() {
		String buff="";
		for(int i=0; i < 10; i++) {
			buff += "[" + data.get(i).k + ", " + data.get(i).v + "]";
		}
		return buff;
	}

}
