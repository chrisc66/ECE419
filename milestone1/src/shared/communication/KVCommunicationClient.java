package shared.communication;

import app_kvServer.KVServer;
import shared.messages.KVMessage;
import shared.messages.KVMessageClass;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class represents communication between server and client.
 * Client side implementation includes KVMessage handling, sending
 * and receiving at the client application. 
 */
public class KVCommunicationClient implements IKVCommunication {
    
    private static Logger logger = Logger.getRootLogger();
    private static final int MAX_KEY_SIZE = 20;
    private static final int BUFFER_SIZE = 1024;
    private static final int MAX_BUFF_SIZE = 120 * BUFFER_SIZE;

    private Socket clientSocket;
    private boolean open;

    private InputStream input;
    private OutputStream output;

    public KVCommunicationClient(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.open = true;
        try {
            this.input = clientSocket.getInputStream();
            this.output = clientSocket.getOutputStream();
        }
        catch (IOException e) {
            logger.error("Error! Connection could not be established!", e);
        }
    }
    
    public boolean isOpen() {
        return this.open;
    }

    public void send(KVMessage message) throws IOException {
        byte[] messageBytes = message.getMessageBytes();
		output.write(messageBytes, 0, messageBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ message.getMessage() +"'");
    }

    public KVMessage receive() throws IOException, Exception{
        int index = 0;
        byte[] msgBytes = null;
        byte[] tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
        
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
        
        while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if (msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } 
                else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is MAX_BUFF_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= MAX_BUFF_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
        
        if (msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } 
        else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		KVMessageClass msg = new KVMessageClass(msgBytes);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMessage().trim() + "'");
		return msg;
    }

    public void close() throws IOException {
        try {
            if (clientSocket != null) {
                clientSocket.close();
            }
            if (input != null){
                input.close();
            }
            if (output != null){
                output.close();
            }
        }
        catch (IOException e) {
            logger.error("Unable to close connection.", e);
        }
    }

}
