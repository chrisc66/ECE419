package client;

import shared.messages.KVMessage;
import shared.communication.KVCommunicationClient;
import shared.messages.KVMessageClass;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface, Runnable {
	
	private static Logger logger = Logger.getRootLogger();
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private KVCommunicationClient kvCommunication;
	private String serverAddress;
	private int serverPort;
	private int total_clients;		// only used for unit testing
	private int clientID;			// only used for unit testing
	private boolean testSuccess;		// only used for unit testing

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
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			System.out.println("Connection is established! Server address = "+ serverAddress +", port = "+serverPort);
			logger.info("Connection is established! Server address = "+ serverAddress +", port = "+serverPort);
		}
		catch (UnknownHostException e) {
			logger.error("UnknownHostException occured!", e);
			throw new UnknownHostException();
		} 
		catch (IllegalArgumentException e) {
			logger.error("IllegalArgumentException occured!", e);
			throw new IllegalArgumentException();
		} 
		catch (Exception e) {
			logger.error("Exception occured!", e);
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
		kvCommunication.send(kvmessage);
		return kvCommunication.receive();
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.GET, key, "");
		kvCommunication.send(kvmessage);
		return kvCommunication.receive();
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
}
