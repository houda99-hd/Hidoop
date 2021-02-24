package hdfs;
import java.io.*;
import java.net.*;
import java.io.Serializable;

public class Message implements Serializable {

    private Commande commande;
    private String nomFichier;
    private int tailleFichier;//Ne sera pas utilisé car la classe Data est sérializable. 
    //Donc pas besoins de connaitre la taille des données pour recevoir

    public Message(Commande c, String nom, int taille ) {
        this.commande = c;
        this.nomFichier = nom;
        this.tailleFichier = taille;
    }

    public Commande getCommande() {
        return this.commande;
    }

    public String getNom() {
        return this.nomFichier;
    }

    public int getTaille() {
        return this.tailleFichier;
    }

    public void setCommande(Commande c) {
    	this.commande = c;
    }

    public void setNom(String n) {
    	this.nomFichier = n;
    }

    public void setTaille(int taille) {
    	this.tailleFichier = taille;
    }
    
    public String toString() {
    	return "Message: cmd = " + this.commande + ", nom fichier = " + this.nomFichier + ", taille fichier = " + this.tailleFichier;
    }

}
