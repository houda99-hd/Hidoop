package hdfs;
import java.io.*;
import java.net.*;
import java.net.ServerSocket;
import java.net.Socket;

public class DataNode {
    private static Socket nameNode;
    private static ServerSocket dataNode;
    public static final int PORT = 8080;
        
    public static void main(String args[]) {
            try {
                ServerSocket dataNode = new ServerSocket(PORT);
                Systems.out.println("Waiting for client request");
    
                while (true) { 
                    Socket s = dataNode.accept();
                    OutputStream os = s.getOutputStream();
                    InputStream is = s.getIntputStream();
                    DataInputStream dis = new DataInputStream(is);
                    Socket data = dis.accept();
                    System.out.println("Accepted connection request");
                    RequestHandler obj = new RequestHandler(data);
                    obj.getName();
                }
            } catch (Exception e) {
                System.out.println(e);
            }
        }
}
