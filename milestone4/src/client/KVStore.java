package client;

import org.apache.log4j.Logger;
import shared.communication.KVCommunicationClient;
import shared.messages.KVMessage;
import shared.messages.KVMessageClass;
import shared.messages.Metadata;

import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class KVStore implements KVCommInterface, Runnable {
	
	/* M1: non-distributed storage service */
	private static Logger logger = Logger.getRootLogger();
	private Socket clientSocket;
	private KVCommunicationClient kvCommunication;
	private String serverAddress;
	private int serverPort;
	private static final String PROMPT = "Client> ";

	/* M2, M3: distributed storage service */
	private List<Metadata> metadata;
	
	/* M4: Data subscription mechanism */
	private Thread clientListenerThread;
	private List<String> subscribtionList;
	private boolean subscribingAll;
	public volatile KVMessage recvMessage; 	// record last received message
	public volatile boolean newMessage; 	// flag for unread new message
	
	/* Unit testing variables */
	private int total_clients;				// total number of clients
	private int clientID;					// cliend identifier within all clients
	private boolean testSuccess;			// flag for test success

	/**
	 * Initialize KVStore with KVServer address and port.
	 * Default constructor for initializing normal client.
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.serverAddress = address;
		this.serverPort = port;
		this.total_clients = -1;
		this.clientID = -1;
		this.testSuccess = true;
		this.subscribtionList = new ArrayList<>();
		this.subscribingAll = false;
		this.recvMessage = null;
		this.newMessage = false;
	}

	/**
	 * Initialize KVStore with KVServer address, port and additional testing information.
	 * Second constructor only for unit testing.
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 * @param total_clients the total number of clients (for unit test only)
	 * @param clientID the identifier of this client (for unit test only)
	 */
	public KVStore(String address, int port, int total_clients, int clientID) {
		this.serverAddress = address;
		this.serverPort = port;
		this.total_clients = total_clients;
		this.clientID = clientID;
		this.testSuccess = true;
		this.subscribtionList = new ArrayList<>();
		this.subscribingAll = false;
		this.recvMessage = null;
		this.newMessage = false;
	}

	@Override
	public void connect() throws Exception {
		try {
			clientSocket = new Socket(serverAddress, serverPort);
			kvCommunication = new KVCommunicationClient(clientSocket, this);
			clientListenerThread = new Thread(kvCommunication);
			clientListenerThread.start();
			System.out.println("Connection is established! Server address = "+ serverAddress +", port = "+serverPort);
			logger.info("Connection is established! Server address = "+ serverAddress +", port = "+serverPort);
		}
		catch (UnknownHostException e) {
			logger.error("UnknownHostException occured!");
			throw new UnknownHostException();
		} 
		catch (IllegalArgumentException e) {
			logger.error("IllegalArgumentException occured!");
			throw new IllegalArgumentException();
		}
		catch (Exception e) {
			logger.error("Exception occured!");
			throw new Exception(e);
		}
	}

	@Override
	public void disconnect() {
		if (isRunning()){
			try {
				KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.DISCONNECT, "", "");
				kvCommunication.send(kvmessage);
				// kvCommunication.receive();
				kvCommunication.close();
				logger.debug("Disconnected from server.");
			}
			catch (Exception e) {
				System.out.println("Error! Close Socket Failed!");
				logger.error("Close Socket Failed!");
			}
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.PUT, key, value);
		return sendKVmessage (kvmessage, key);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.GET, key, "");
		return sendKVmessage (kvmessage, key);
	}

	public boolean isRunning() {
		return (kvCommunication != null) && kvCommunication.isOpen();
	}

	public boolean testSuccess() {
		return this.testSuccess;
	}

	/**
	 * Only used for unit testing.
	 * Tests multiple clients that connect to one server.
	 */
	public void run() {
		try {
			String getVal = "";
			connect();
			for (int i = clientID; i < clientID + total_clients; i ++){
				put("key" + i, "value" + i);
			}
			for (int i = clientID; i < clientID + total_clients; i ++){
				getVal = get("key" + i).getValue();
				if (!getVal.equals("value" + i)){
					testSuccess = false;
				}
			} 
			disconnect();
		}
		catch (Exception e){
			testSuccess = false;
		}
	}

	/**
	 * helper function for getting MD5 hash key
	 * may need to move to some shared class for being visible for both client and server
	 */

	public BigInteger mdKey (String key) throws NoSuchAlgorithmException {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md_key = md.digest(key.getBytes());
			BigInteger md_key_bi = new BigInteger(1, md_key);
			return md_key_bi;
		} catch (NoSuchAlgorithmException e) {
			logger.error("NoSuchAlgorithmException occured!");
			throw new NoSuchAlgorithmException();
		}
	}

	/**
	 * helper function for connecting correct server
	 * this function would update metadata and create a new connection
	 */

	public void updateServer (KVMessage msg, String key) throws NoSuchAlgorithmException, Exception {
		metadata = msg.getMetadata();
		for (Metadata m : metadata){
			logger.info("Printing updated metadata: " + m.serverAddress + " " + m.port + " " + m.start + " " + m.stop);
		}
		BigInteger key_bi = mdKey(key);
		for (int i = 0; i < metadata.size(); i++ ) {
			Metadata obj = metadata.get(i);
			// START <= STOP && key > START && key < STOP
			// START >= STOP && key > START && key > STOP
			// START >= STOP && key < START && key < STOP
			if ((obj.start.compareTo(obj.stop) !=  1) && (key_bi.compareTo(obj.start) ==  1 && key_bi.compareTo(obj.stop) == -1) || 
				(obj.start.compareTo(obj.stop) != -1) && (key_bi.compareTo(obj.start) ==  1 && key_bi.compareTo(obj.stop) ==  1) || 
				(obj.start.compareTo(obj.stop) != -1) && (key_bi.compareTo(obj.start) == -1 && key_bi.compareTo(obj.stop) == -1) ){
				disconnect();
				String lastServerAddress = serverAddress;
				int lastServerPort = serverPort;
				serverAddress = obj.serverAddress;
				serverPort = obj.port;
				logger.info("Connecting to another server, " + serverAddress + ":" + serverPort);
				try {
					connect();
				} catch (Exception e) {
					serverAddress = lastServerAddress;
					serverPort = lastServerPort;
					connect();
					logger.error("New connection to a metadata server failed, back to last server");
				}
				return;
			}
		}
		logger.error("metadata not correct, did not find a correct server");
	}

	/**
	 * helper functions for sending KVmessage
	 */

	public KVMessage sendKVmessage (KVMessage kvmessage, String key) throws Exception {
		
		newMessage = false;
		kvCommunication.send(kvmessage);

		// wait for new message before return (set by listener thread)
		while (!newMessage){}
		newMessage = false;

		return recvMessage;
	}


	/**
	 * In the case of broken server connection, this function uses a recursive structure to 
	 * reconnect KVStore to another KVServer on its metadatas record. 
	 * 
	 * @param sendMsg KVMessage send to KVServer.
	 * @param i Number of recursive call. 
	 * @return KVMessage received from new KVServer.
	 * @throws Exception Throws exception upon reconnection failure, exit the program.  
	 */
	public KVMessage reconnectAndReceive(KVMessage sendMsg, int i) throws Exception {
		
		System.out.println("Cannot find server. Reconnecting ... ");
		logger.info("Cannot find server. Reconnecting ... ");
		if (metadata == null || i >= metadata.size()){
			logger.error("Cannot find avaliable server to connect, connection closed");
			System.out.println("Cannot find avaliable server to connect, connection closed");
			System.out.print(PROMPT);
			throw new Exception("Cannot find avaliable server to connect, connection closed");
		}

		KVMessage recvMsg = null;
		this.serverAddress = metadata.get(i).serverAddress;
		this.serverPort = metadata.get(i).port;

		disconnect();
		try {
			connect();
		}
		catch (Exception e){
			recvMsg = reconnectAndReceive(sendMsg, i + 1);
		}
		
		return recvMsg;
	}

	public void subscribe(boolean all){
		subscribingAll = true;
	}

	public void subscribe(String key){
		subscribtionList.add(key);
	}
	
	public void unsubscribe(boolean all){
		subscribingAll = false;
		subscribtionList.clear();
	}

	public void unsubscribe(String key){
		subscribtionList.remove(key);
	}

	public boolean subscribed(String key){
		if (subscribingAll){
			return true;
		}
		return subscribtionList.contains(key);
	}

}

