package client;

import shared.messages.KVMessage;
import shared.communication.KVCommunicationClient;
import shared.messages.KVMessageClass;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class KVStore implements KVCommInterface {
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private KVCommunicationClient kvCommunication;
	private String serverAddress;
	private int serverPort;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		serverAddress = address;
		serverPort = port;
//		try {
//			clientSocket = new Socket(address, port);
//		} catch (Exception e) {
//			System.out.println("new Socket fails");
//		}
	}

	@Override
	public void connect() throws Exception {
//		clientSocket = new Socket(serverAddress, serverPort);
		try {
			System.out.println("serverAddress = "+ serverAddress +" serverPort = "+serverPort);
			clientSocket = new Socket(serverAddress, serverPort);
			System.out.println("new Socket");
			kvCommunication = new KVCommunicationClient(clientSocket);
			System.out.println("new KVCommunicationClient");
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			System.out.println("Connection is established! \t output stream = " + output);
		}
		catch (UnknownHostException unknownE) {
//			System.out.println("Connection Failed!");
			System.err.println("In catch IOException: "+unknownE.getClass());
			throw new UnknownHostException();
		} catch (IllegalArgumentException illegalArgE) {
			System.err.println("In catch IOException: "+illegalArgE.getClass());
			throw new IllegalArgumentException();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Socket error");
			throw new Exception();
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
			}
			catch (Exception e) {
				System.out.println("Close Socket Failed!");
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
}
