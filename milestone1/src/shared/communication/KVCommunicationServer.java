package shared.communication;

import app_kvServer.KVServer;
import shared.messages.KVMessage;
import shared.messages.KVMessageClass;
import shared.messages.KVMessage.StatusType;

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
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

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

    public KVMessage receive() throws IOException, Exception {
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
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
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

    public void close() {
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
        String sendMsgKey = "";
        String sendMsgValue = "";
        switch(message.getStatus()){
            case GET: 
                // Aquire key-value pair from the server
                try {
                    sendMsgValue = kvServer.getKV(message.getKey());
                    sendMsgType = StatusType.GET_SUCCESS;
                    logger.info("GET SUCCESS: Value is found on server, key: " + message.getKey());
                }
                catch (Exception e) {
                    sendMsgType = StatusType.GET_ERROR;
                    logger.info("GET ERROR: Value not found on server, key: " + message.getKey(), e);
                }
                break;
            case PUT: 
                // Identify status type and store key-value pair on the server
                try {
                    try {
                        kvServer.getKV(message.getKey());
                        if (message.getValue() != null)
                            sendMsgType = StatusType.PUT_UPDATE;
                        else 
                            sendMsgType = StatusType.DELETE_SUCCESS;
                        logger.info("PUT UPDATE: Value is updated on server, key: " + message.getKey());
                    } 
                    catch (Exception e) {   // getValue() exception when value not found
                        if (message.getValue() != null)
                            sendMsgType = StatusType.PUT_SUCCESS;
                        else
                            sendMsgType = StatusType.DELETE_SUCCESS;
                        logger.info("PUT SUCCESS: Value is stored on server, key: " + message.getKey());
                    }
                    kvServer.putKV(message.getKey(), message.getValue());
                }
                catch (Exception e) {
                    if (message.getValue() != null)
                        sendMsgType = StatusType.PUT_ERROR;
                    else
                        sendMsgType = StatusType.DELETE_ERROR;
                    ////TODO: if falls into this exception, logger will be printed twice
                    logger.info("PUT ERROR: Value cannot be stored on server, key: " + message.getKey(), e);
                }
                break;
            default:
                // Server only handles GET and PUT, not handling other message types
                throw new IllegalStateException("Received an unsupported messaage. Server only handles 'GET' and 'PUT' KVMessages.");
        }
        return new KVMessageClass(sendMsgType, sendMsgKey, sendMsgValue);
    }

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
