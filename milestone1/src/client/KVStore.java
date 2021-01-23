package client;

import shared.messages.KVMessage;
import shared.communication.KVCommunicationClient;
import shared.messages.KVMessageClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


public class KVStore implements KVCommInterface {
//	private String serverAddress;
//	private int serverPort;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private KVCommunicationClient kvCommunication;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
//		serverAddress = address;
//		serverPort = port;
		try {
			clientSocket = new Socket(address, port);
			kvCommunication = new KVCommunicationClient(clientSocket);
		} catch (Exception e) {
			System.out.println("Socket is created!");
		}
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
//		clientSocket = new Socket(serverAddress, serverPort);
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			System.out.println("Connection is established!\n , output stream = " + output);
		} catch (Exception e) {
			System.out.println("Connection Fails!");
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
//		if (clientSocket != null) {
//			try {
//				clientSocket.close();
//			} catch (IOException e) {
//				System.out.println("Disconnection Fails!");
//			}
//			clientSocket = null;
//			System.out.println("Disconnected!");
//		}
		if (isRunning()){
			try {
				KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.DISCONNECT, "", "");
				kvCommunication.send(kvmessage);
				kvCommunication.receive();
				kvCommunication.close();
			} catch (Exception e) {
				System.out.println("Close Socket Failed!");
			}
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.PUT, key, value);
		kvCommunication.send(kvmessage);
		return kvCommunication.receive();
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		KVMessageClass kvmessage = new KVMessageClass(KVMessage.StatusType.GET, key, "");
		kvCommunication.send(kvmessage);
		return kvCommunication.receive();
	}

	public boolean isRunning() {
		return kvCommunication.isOpen();
	}
}
