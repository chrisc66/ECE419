package app_kvServer;

import shared.communication.KVCommunicationServer;
import logger.LogSetup;

import java.util.ArrayList;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.net.BindException;
import java.net.Socket;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer implements IKVServer {
	
	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean running;
	private ArrayList<Thread> clientThreads;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.serverSocket = null;
		this.port = port;
		this.cacheSize = cacheSize;
		this.clientThreads = new ArrayList<Thread>();
	}
	
	@Override
	public int getPort(){
		return this.port;
	}

	@Override
    public String getHostname(){
		String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			logger.error("The IP address of server host cannot be resolved. \n", e);
		}
		return hostname;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		switch(this.strategy){
			case "None":
				return IKVServer.CacheStrategy.None;
			case "LRU":
				return IKVServer.CacheStrategy.LRU;
			case "LFU":
				return IKVServer.CacheStrategy.LFU;
			case "FIFO":
				return IKVServer.CacheStrategy.FIFO;
			default:
				logger.error("Undefined use of IKVServer.CacheStrategy, setting to None.");
				return IKVServer.CacheStrategy.None;	
		}
	}

	@Override
    public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		////TODO seach for key and return boolean
		return false;
	}

	@Override
    public boolean inCache(String key){
		////TODO seach for key and return boolean
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		////TODO search for key and return its value
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		////TODO store key-value pair
	}

	@Override
    public void clearCache(){
		////TODO clear cache
	}

	@Override
    public void clearStorage(){
		////TODO clear storage
	}

	@Override
    public void run(){
		
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " + serverSocket.getLocalPort());    
			running = true;
		}
		catch (IOException e) {
			logger.error("Error! Cannot open server socket. \n", e);
			if (e instanceof BindException){
             	logger.error("Port " + port + " is already bound! \n");
            }
			running = false;
        }
        
        if (serverSocket != null) {
	        while (running){
	            try {
					Socket clientSocket = serverSocket.accept();
					KVCommunicationServer communication = new KVCommunicationServer(clientSocket, this);
	                Thread clientThread = new Thread(communication);
	                clientThread.start();
	                clientThreads.add(clientThread);              
					logger.info("Connected to " + clientSocket.getInetAddress().getHostName() +  
							" on port " + clientSocket.getPort());
				} 
				catch (IOException e) {
					logger.error("Error! Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
	}

	@Override
    public void kill(){
		////TODO save to storage
		////TODO force shut down server
		running = false;
		try {
			serverSocket.close();
		} 
		catch (IOException e) {
			logger.error("Error! Unable to close socket on port: " + port, e);
		}
	}

	@Override
    public void close(){
		////TODO perform any action
		////TODO gracefully shut down server
		running = false;
		try {
			for (int i = 0; i < clientThreads.size(); i++){
                clientThreads.get(i).interrupt();
            }
			serverSocket.close();
		} 
		catch (IOException e) {
			logger.error("Error! Unable to close socket on port: " + port, e);
		}
	}
}
