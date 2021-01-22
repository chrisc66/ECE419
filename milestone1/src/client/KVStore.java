package client;

import shared.messages.KVMessage;

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
		} catch (Exception e) {
			System.out.println("Socket is created!");
		}
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
//		clientSocket = new Socket(serverAddress, serverPort);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		System.out.println("Create socket successfully, connection is established!\n , output stream = " + output );
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if (clientSocket != null) {
			try {
				clientSocket.close();
			} catch (IOException e) {
				System.out.println("Disconnection Fails!");
			}
			clientSocket = null;
			System.out.println("Disconnected!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isRunning() throws Exception {
		return clientSocket.isClosed();
	}
}
