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
    private static final int MAX_BUFF_SIZE = 128 * BUFFER_SIZE;

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
        
        System.out.println("KVCommunicationServer send() entering ...");
        
        byte[] messageBytes = message.getMessageBytes();
		output.write(messageBytes, 0, messageBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
                + message.getMessage() +"'");
        
        System.out.println("KVCommunicationServer send() leaving ...");
    }

    public KVMessage receive() throws IOException, Exception {
        
        System.out.println("KVCommunicationServer receive() entering ...");
        
        int index = 0;
        byte[] msgBytes = null;
        byte[] tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
        
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
        int numDeliminator = 0;

        while (reading) {

            System.out.println("KVCommunicationServer receive() reading ...");

            if (read == 10){
                numDeliminator ++;
            }
            if (numDeliminator == 3 || read == -1){
                break;
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
			
			/* stop reading is MAX_BUFF_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= MAX_BUFF_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
        
        System.out.println("KVCommunicationServer receive() line 122");

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
        System.out.println("KVCommunicationServer receive() line 147");
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMessage().trim() + "'");
        
        System.out.println("KVCommunicationServer receive() leaving ...");
        
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
        
        System.out.println("KVCommunicationServer process() entering ...");

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
            case DISCONNECT:
                sendMsgType = StatusType.DISCONNECT;
                open = false;
                break;
            default:
                // Server only handles GET and PUT, not handling other message types
                throw new IllegalStateException("Received an unsupported messaage. Server only handles 'GET' and 'PUT' KVMessages.");
        }

        KVMessage sendMsg = new KVMessageClass(sendMsgType, sendMsgKey, sendMsgValue);

        System.out.println("KVCommunicationServer process() entering ...");

        return sendMsg;
    }

    public void run(){
        // KVCommunicationServer process runs in this loop
        while (open) {
            System.out.println("KVCommunicationServer run() starting ...");
            try {
                KVMessage recvMsg = receive();          // listening / waiting on receive()
                System.out.println("KVCommunicationServer run() line 214");
                KVMessage sendMsg = process(recvMsg);
                System.out.println("KVCommunicationServer run() line 216");
                send(sendMsg);
                System.out.println("KVCommunicationServer run() line 218");
            }
            catch (IOException e) {
                logger.error("Server lost client lost! ", e);
                open = false;
            }
            catch (Exception e) {
                logger.error(e);
            }
            System.out.println("KVCommunicationServer run() finishing ...");
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
