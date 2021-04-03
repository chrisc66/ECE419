package shared.communication;

import org.apache.log4j.Logger;

import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageClass;
import shared.messages.KVAdminMessage.KVAdminType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * This class represents communication between server and client.
 * Client side implementation includes KVMessage handling, sending
 * and receiving at the client application. 
 */
public class KVCommunicationClient implements IKVCommunication, Runnable {
    
    private static Logger logger = Logger.getRootLogger();
    private static final int BUFFER_SIZE = 1024;
    private static final String PROMPT = "Client> ";

    private KVStore kvStore;
    private KVMessage lastSentMessage;

    private Socket clientSocket;
    private boolean open;

    private InputStream input;
    private OutputStream output;

    public KVCommunicationClient(Socket clientSocket, KVStore kvStore) {
        this.kvStore = kvStore;
        this.clientSocket = clientSocket;
        this.open = true;
        try {
            this.input = clientSocket.getInputStream();
            this.output = clientSocket.getOutputStream();
            logger.info("Opening connection.");
        }
        catch (IOException e) {
            logger.error("Connection could not be established!", e);
        }
    }
    
    public boolean isOpen() {
        return this.open;
    }

    public void send(KVMessage message) throws IOException {
        if (message.getStatus() == KVMessage.StatusType.PUT || message.getStatus() == KVMessage.StatusType.GET){
            lastSentMessage = message;
        }
        byte[] messageBytes = message.getMessageBytes();
		output.write(messageBytes, 0, messageBytes.length);
		output.flush();
        logger.debug("SEND <" + clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" + message.getMessage() +"'");
    }

    public KVMessage receive() throws IOException, Exception {

        int index = 0;
        byte[] msgBytes = null;
        byte[] tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
        
		/* read first char from stream */
        byte read = 0;  // read from input stream
        byte prev = 0;  // prev of read
        byte copy = 0;  // copy of read
        boolean reading = true;
        int numDeliminator = 0;
        while (reading && numDeliminator != 3) {

            /* read next char from stream */
            prev = copy;
            read = (byte) input.read();
            copy = read;

            // "D" = 68, "\n" = 10
            if (prev == 68 && copy == 10){
                numDeliminator ++;
            }

            if (read == -1){
                return new KVMessageClass(StatusType.DISCONNECT, "", "");
            }
            
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
		KVMessage msg = new KVMessageClass(msgBytes);
		logger.debug("RECEIVE <" + clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" + msg.getMessage().trim() + "'");

        return msg;
    }

    public void close() throws IOException {
        open = false;
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
            logger.info("Closing connection.");
        }
        catch (IOException e) {
            logger.error("Unable to close connection.", e);
        }
    }

    @Override
    public void run() {
        while (open){
            try {
                KVMessage recvMsg = receive();
                logger.info(recvMsg.toString());
                if (recvMsg.getStatus() == KVMessage.StatusType.SUBSCRITION_UPDATE){
                    System.out.println("Data Update: key = " + recvMsg.getKey() + ", value = " + recvMsg.getValue());
                }
                switch (recvMsg.getStatus()){
                    case PUT_SUCCESS:
                        logger.info(recvMsg.getStatusString());
                        break;
                    case PUT_UPDATE:
                        logger.info(recvMsg.getStatusString());
                        break;
                    case PUT_ERROR:
                        printError("Received message: " + recvMsg.getStatusString());
                        logger.error(recvMsg.getStatusString());
                        break;
                    case GET_SUCCESS:
                        logger.info(recvMsg.getStatusString());
                        break;
                    case GET_ERROR:
                        printError("Received message: " + recvMsg.getStatusString());
                        logger.error(recvMsg.getStatusString());
                        break;
                    case DELETE_SUCCESS:
                        logger.info(recvMsg.getStatusString());
                        break;
                    case DELETE_ERROR:
                        printError("Received message: " + recvMsg.getStatusString());
                        logger.error(recvMsg.getStatusString());
                        break;
                    case DISCONNECT:
                        open = false;
                        kvStore.reconnectAndReceive(lastSentMessage, 0);
                        System.out.print(PROMPT);
                        return;
                    case SERVER_STOPPED:
                        printError("Received message: " + recvMsg.getStatusString());
                        logger.error(recvMsg.getStatusString());
                        break;
                    case SERVER_WRITE_LOCK:
                        printError("Received message: " + recvMsg.getStatusString());
                        logger.error(recvMsg.getStatusString());
                        break;
                    case SERVER_NOT_RESPONSIBLE:
                        kvStore.updateServer(recvMsg, lastSentMessage.getKey());
                        kvStore.sendKVmessage(lastSentMessage, lastSentMessage.getKey());
                        break;
                    case SUBSCRITION_UPDATE:
                        System.out.println("Received message: " + recvMsg.getStatusString());
                        break;
                    default:
                        printError("Received message: " + recvMsg.getStatusString());
                        logger.error(recvMsg.getStatusString());
                }
            }
            catch (Exception e){
                printError("Exception occured, closing server connection.");
            }
            System.out.print(PROMPT);
        }
    }

    private void printError(String error){
        System.out.println("Error! " +  error);
    }

}
