package shared.communication;

import app_kvServer.KVServer;
import shared.messages.KVMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
// import java.io.DataInputStream;
// import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVCommunication implements IKVCommunication {
    
    private static Logger logger = Logger.getRootLogger();
    private static final int MAX_BUFF_SIZE = 1024;
    private Socket clientSocket;
    private KVServer kvServer;
    private boolean opened;
    protected BufferedInputStream inputStream;
    protected BufferedOutputStream outputStream;

    public KVCommunication(Socket clientSocket, KVServer server) {
        this.clientSocket = clientSocket;
        this.kvServer = server;
        this.opened = true;
        try {
            this.outputStream = (BufferedOutputStream) clientSocket.getOutputStream();
            this.inputStream = (BufferedInputStream) clientSocket.getInputStream();
        }
        catch (IOException e) {
            logger.error("Error! Connection could not be established!", e);
        }
    }
    
    @Override
    public void send(KVMessage msg) throws IOException {

    }

    @Override
    public KVMessage recv() throws IOException {
        return null;
    }

}
