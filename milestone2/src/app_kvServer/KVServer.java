package app_kvServer;

import shared.communication.KVCommunicationServer;
import shared.messages.KVMessage;
import shared.messages.Metadata;
import logger.LogSetup;
import DiskStorage.DiskStorage;

import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.net.*;
import java.io.IOException;
import java.io.File;

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer implements IKVServer, Runnable {

	private static Logger logger = Logger.getRootLogger();

	// M1: KVServer config
	private ServerSocket serverSocket;
	private int port;
	private int cacheSize;
	private String strategy;
	private boolean running;

	// M1: KVClient connections
	private ArrayList<Thread> clientThreads;

	// M1: KVServer disk persistent storage
	private DiskStorage diskStorage;
	private static final String dir = "./data";
	private static final String filePreFix = "persistanceDB.properties";

	// M2: Distributed server config
	public static ArrayList<Metadata> metadataList = new ArrayList<Metadata>();
	private Metadata serverMetadata;	// metadata for itself
	private boolean distributed;
	private String serverName;
	private boolean writeLock;
	private String zkHostname;
    private int zkPort;
    private static final int zkTimeout = 20000;
	private static final String zkRootNodePath = "/StorageServerRoot";
	private ZooKeeper zk;

	/**
	 * Start KV Server at given port. 
	 * Note: this constructor creates a non-distributed KVServer object. 
	 * 
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.distributed = false;
		this.serverSocket = null;
		this.port = port;
		this.cacheSize = cacheSize;
		this.clientThreads = new ArrayList<Thread>();
		this.serverName = getHostname()+":"+getPort();
		if (storageFileExist())
			this.diskStorage = new DiskStorage(filePreFix, serverName);
		else
			this.diskStorage = new DiskStorage(serverName);
		// Create a new thread that start runing KVServer
		Thread clientThread = new Thread(this);
		clientThread.start();
	}

	/**
	 * Start KV Server with ZooKeeper instance.
	 * Note: this constructor creates a distributed KVServer object. 
	 * 
	 * @param serverName KVServer name in the form of ip:port
	 * @param zkPort ZooKeeper port
	 * @param zkHostname ZooKeeper host name
	 */
	public KVServer(String serverName, int zkPort, String zkHostname){
		this.distributed = true;
		this.serverSocket = null;
		this.port = Integer.parseInt(serverName.split(":")[2]);	// port is contained in server name
		this.cacheSize = 0;
		this.clientThreads = new ArrayList<Thread>();
		this.serverMetadata = null;
		this.writeLock = false;
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
		// Persistent disk storage
		if (storageFileExist())
			this.diskStorage = new DiskStorage(filePreFix, serverName);
		else
			this.diskStorage = new DiskStorage(serverName);
		// ZooKeeper client
		try{
            final CountDownLatch latch = new CountDownLatch(1);
			this.zk = new ZooKeeper(zkHostname+":"+zkPort, zkTimeout, new Watcher(){
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected)
                        latch.countDown();
                }
            });
            latch.await();
        } catch (IOException | InterruptedException e){
            logger.error(e);
        }
		// ZNode on ECS ZooKeeper server
		try {
			if (zk.exists(zkRootNodePath+"/"+serverName, false) == null) {
				zk.create(zkRootNodePath+"/"+serverName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		// Metadata
		try {
			final String zkNodePath = zkRootNodePath+"/"+serverName;
			byte[] kvAdminMsgBytes = zk.getData(zkNodePath, new Watcher() {
				// handle hashRing update
				public void process(WatchedEvent we) {
					if (!running) return;
					try {
						byte[] kvAdminMsgBytes = zk.getData(zkNodePath, this, null);
						setMetadata(kvAdminMsgBytes);
					} catch (KeeperException | InterruptedException e) {
						logger.error(e);
					}
				}
			}, null);
			setMetadata(kvAdminMsgBytes);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		// Run KVServer after creation
		this.run();
	}

	/** 
	 * Update metadata list and servermetadata from byte array.
	 */
	public void setMetadata(byte[] kvAdminMsgBytes){
		// TODO: set metadata list using KVAdminMessage bytes
		// this.metadataList = 
		// this.serverMetadata = 
	}

	private boolean storageFileExist(){
		File dirFIle = new File(dir);
		if (!dirFIle.exists()){
			return false;
		}
		else {
			File dummyFile = new File(dir+'/'+filePreFix+serverName);
			return dummyFile.exists();
		}
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
		}
		catch (UnknownHostException e) {
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
				logger.debug("Undefined use of IKVServer.CacheStrategy, setting to None.");
				return IKVServer.CacheStrategy.None;
		}
	}

	@Override
	public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
	public boolean inStorage(String key){
		return diskStorage.onDisk(key);
	}

	@Override
	public boolean inCache(String key){
		// return false since cache is not yet implemented
		return false;
	}

	@Override
	public String getKV(String key) throws Exception{
		String value = diskStorage.get(key);
		if (value == null){
			logger.debug("Key " + key + " cannot be found on server");
			throw new Exception("Key cannot be found on server");
		}
		return value;
	}

	@Override
	public boolean deleteKV(String key) throws Exception{
		return diskStorage.delelteKV(key);
	}

	@Override
	public void putKV(String key, String value) throws Exception{
		boolean success = diskStorage.put(key, value);
		if (success == false){
			logger.debug("Unable to put key value pair into storage. Key = " + key + ", Value = " + value);
			throw new Exception("Unable to put key value pair into storage.");
		}
	}

	@Override
	public void clearCache(){
		// do nothing since cache is not yet implemented
	}

	@Override
	public void clearStorage(){
		diskStorage.clearDisk();
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
		running = false;
		try {
			for (int i = 0; i < clientThreads.size(); i++){
				clientThreads.get(i).interrupt();	// interrupt and stop all threads
			}
			serverSocket.close();
		}
		catch (IOException e) {
			logger.error("Error! Unable to close socket on port: " + port, e);
		}
	}

	@Override
	public ArrayList<Metadata> getMetaData(){
		return metadataList;
	}

	@Override
	public Map<String, String> getKVOutOfRange(){
		return diskStorage.getKVOutOfRange(serverMetadata.start, serverMetadata.stop);
	}

	@Override
	public void aquireWriteLock(){ writeLock = true; }

	@Override
	public void releaseWriteLock(){ writeLock = false; }

	@Override
	public boolean getWriteLock(){ return writeLock; }

	public static void main(String[] args) throws IOException {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			int port = Integer.parseInt(args[0]);
			int cacheSize = Integer.parseInt(args[1]);
			String strategy = args[2];
			new KVServer(port, cacheSize, strategy);
		}
		catch (IOException e) {
			logger.error("Error! Unable to initialize server logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}