package shared.communication;

import app_kvServer.KVServer;
import shared.messages.KVMessage;
import shared.messages.KVMessageClass;
import shared.messages.KVMessage.StatusType;

import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class represents communication between server and client.
 * Server side implementation includes KVMessage handling, sending
 * and receiving at the server application. 
 */
public class KVCommunicationServer implements IKVCommunication, Runnable {
    
    private static Logger logger = Logger.getRootLogger();
    private static final int BUFFER_SIZE = 1024;

    private Socket clientSocket;
    private KVServer kvServer;
    private boolean open;

    private InputStream input;
    private OutputStream output;

    public KVCommunicationServer(Socket clientSocket, KVServer server) {
        this.clientSocket = clientSocket;
        this.kvServer = server;
        this.open = true;
        try {
            this.input = clientSocket.getInputStream();
            this.output = clientSocket.getOutputStream();
            logger.info("Opening connection.");
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
		logger.debug("SEND <" + clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" + message.getMessage() +"'");
    }

    public KVMessage receive() throws IOException, Exception {

        int index = 0;
        byte[] msgBytes = null;
        byte[] tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
        
		/* read first char from stream */
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

    /**
     * Processes messages and perform operation on the server. Waits on server until server returns.
     * 
     * @param message KVMessage received at the server. 
     * 
     * @return KVMessage send back by the server
     * @throws IllegalStateException Illegal message status type exception.
     * @throws Exception Illegal message content lenth.
     */
    public KVMessage process(KVMessage message) throws IllegalStateException, Exception {

        StatusType sendMsgType = StatusType.UNDEFINED;
        String sendMsgKey = message.getKey();   // return the same key as received message
        String sendMsgValue = "";

        switch(message.getStatus()){
            case GET: 
                // Aquire key-value pair from the server
                try {
                    sendMsgValue = kvServer.getKV(message.getKey());
                    sendMsgType = StatusType.GET_SUCCESS;
                    logger.info("GET_SUCCESS: Value is found on server, key: " + message.getKey());
                }
                catch (Exception e) {
                    sendMsgType = StatusType.GET_ERROR;
                    logger.info("GET_ERROR: Value not found on server, key: " + message.getKey());
                }
                break;
            case PUT: 
                // Identify status type and store key-value pair on the server
                if (!message.getValue().equals("")){    // PUT
                    // check if key-value pair is already stored
                    try {
                        kvServer.getKV(message.getKey());
                        sendMsgType = StatusType.PUT_UPDATE;
                    }
                    catch (Exception e){
                        sendMsgType = StatusType.PUT_SUCCESS;
                    }
                    // store / update key-value pair
                    try {
                        kvServer.putKV(message.getKey(), message.getValue());
                        sendMsgValue = message.getValue();
                    }
                    catch (Exception e) {
                        sendMsgType = StatusType.PUT_ERROR;
                    }
                    // set logger message
                    if (sendMsgType == StatusType.PUT_SUCCESS){
                        logger.info("PUT_SUCCESS: Value is stored on server, key: " + message.getKey() + ", value: " + message.getValue());
                    }
                    else if (sendMsgType == StatusType.PUT_UPDATE){
                        logger.info("PUT_UPDATE: Value is updated on server, key: " + message.getKey() + ", value: " + message.getValue());
                    }
                    else{
                        logger.info("PUT_ERROR: Value cannot be stored on server, key: " + message.getKey() + ", value: " + message.getValue());
                    }
                }
                else {  // DELETE
                    try {
                        boolean ret = kvServer.deleteKV(message.getKey());
                        if (ret){
                            sendMsgType = StatusType.DELETE_SUCCESS;
                            logger.info("DELETE_SUCCESS: Value is deleted on server, key: " + message.getKey() + ", value: " + message.getValue());
                        }
                        else {
                            sendMsgType = StatusType.DELETE_ERROR;
                            logger.info("DELETE_ERROR: Value cannot be deleted on server, key: " + message.getKey() + ", value: " + message.getValue());
                        }
                    }
                    catch (Exception e) {
                        sendMsgType = StatusType.DELETE_ERROR;
                        logger.info("DELETE_ERROR: Value cannot be deleted on server, key: " + message.getKey() + ", value: " + message.getValue());
                    }
                }
                break;
            case DISCONNECT:
                open = false;
                sendMsgType = StatusType.DISCONNECT;
                logger.info("PUT_SUCCESS: Value is stored on server");
                break;
            default:
                // Server only handles GET and PUT, not handling other message types
                throw new IllegalStateException("Received an unsupported messaage. Server only handles 'GET' and 'PUT' KVMessages.");
        }

        return new KVMessageClass(sendMsgType, sendMsgKey, sendMsgValue);
    }

    /**
     * This is the run() method for KVCommunicationServer thread. 
     * Waits on incomming KVMessages, process the message upon receiving and send back to client.
     */
    public void run(){
        // KVCommunicationServer process runs in this loop
        while (open) {
            try {
                KVMessage recvMsg = receive();          // listening / waiting on receive()
                KVMessage sendMsg = process(recvMsg);
                send(sendMsg);
            }
            catch (IOException e) {
                logger.error("Server lost client lost! ", e);
                open = false;
            }
            catch (Exception e) {
                logger.error(e);
            }
        }

        // before finish running, close connection
        if (clientSocket != null) {
            try {
                input.close();
                output.close();
                clientSocket.close();
            }
            catch (IOException e) {
                logger.error("Unable to close connection!", e);
            }
        } 
    }

}
