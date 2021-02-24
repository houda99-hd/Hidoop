package ordo;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class NameRegistry {
	
	public static void main(String[] args) throws InterruptedException {
		System.out.println("Création du registre des noms rmi.");
		try {
			Registry registry = LocateRegistry.createRegistry(4000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Thread.sleep(10000000);
	}

}
