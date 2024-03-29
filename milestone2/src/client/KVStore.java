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
import java.util.List;

public class KVStore implements KVCommInterface, Runnable {
	
	private static Logger logger = Logger.getRootLogger();
	private Socket clientSocket;
	private KVCommunicationClient kvCommunication;
	private String serverAddress;
	private int serverPort;
//	private MessageDigest md;//getInstance(String algorithm)
	private List<Metadata> metadata;
	private int total_clients;		// only used for unit testing
	private int clientID;			// only used for unit testing
	private boolean testSuccess;	// only used for unit testing

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
	}

	@Override
	public void connect() throws Exception {
		try {
			clientSocket = new Socket(serverAddress, serverPort);
			kvCommunication = new KVCommunicationClient(clientSocket);
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
				kvCommunication.receive();
				kvCommunication.close();
				logger.debug("Disconnected from server.");
			}
			catch (Exception e) {
				System.out.println("Error! Close Socket Failed!");
				logger.error("Close Socket Failed!", e);
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
		
		// System.out.println("=========================================");
		// System.out.println("KVClient send KVMessage");
		// System.out.println(kvmessage.toString());
		// System.out.println("=========================================");
		
		kvCommunication.send(kvmessage);
		KVMessage msg = kvCommunication.receive();

		// System.out.println("=========================================");
		// System.out.println("KVClient receive KVMessage");
		// System.out.println(msg.toString());
		// System.out.println("=========================================");

		if (msg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			updateServer(msg, key);
			// System.out.println("=========================================");
			// System.out.println("SERVER_NOT_RESPONSIBLE: KVClient send KVMessage");
			// System.out.println(kvmessage.toString());
			// System.out.println("=========================================");
			kvCommunication.send(kvmessage);
			msg = kvCommunication.receive();
			// System.out.println("=========================================");
			// System.out.println("KVClient receive KVMessage");
			// System.out.println(msg.toString());
			// System.out.println("=========================================");
		}
		return msg;
	}

	/**
     * check if request key is in current server hashing range
     */
    // public boolean keyWithinRange(BigInteger key_hash){

	// 	BigInteger max = new BigInteger(0xffffffffffffffffffffffffffffffff);
		
    //     List<BigInteger> hashRing = new ArrayList<>();
    //     Iterator it = metadata.iterator();
	// 	while (it.hasNext()) {
	// 		Metadata entry = (Metadata)it.next();
    //         hashRing.add(entry.start);
	// 	}
    //     Collections.sort(hashRing);
    //     int i;
    //     for (i = hashRing.size() - 1; i != 0; i --){
    //         int compare = key_hash.compareTo(hashRing.get(i));
	// 		if (compare >= 0){
    //             break;   
    //         }
    //     }
	// 	int my_idx;
	// 	for (my_idx = 0; i < hashRing.size(); i ++){
	// 		Stting servername = serverAddress+":"+serverPort;
	// 		if (hashRing.get(i).compareTo(metadata.get(my_idx).start) == 0){
	// 			break;
	// 		}
	// 	}
    //     int compare = hashRing.get(i).compareTo(metadata.get(my_idx).start);
    //     return (compare == 0);
    // }
}
