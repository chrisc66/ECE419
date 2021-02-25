package app_kvServer;

import shared.communication.KVCommunicationServer;
import shared.messages.KVAdminMessage;
import shared.messages.KVMessage;
import shared.messages.Metadata;
import shared.messages.KVAdminMessage.KVAdminType;
import logger.LogSetup;
import DiskStorage.DiskStorage;

import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.net.*;
import java.io.IOException;
import java.math.BigInteger;
import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer implements IKVServer, Runnable {

	private static Logger logger = Logger.getRootLogger();

	// M1: KVServer config
	private ServerSocket serverSocket;											// socket that KVServer listens to
	private int port;															// port that KVServer listens on
	private int cacheSize;														// KVServer cache size (not implemented)
	private String strategy;													// KVServer cache strategy (not implemented)
	private boolean running;													// flag to indicate if KVServer is running 

	// M1: KVClient connections
	private ArrayList<Thread> clientThreads;									// list of active client threads (KVCommunicationServer)

	// M1: KVServer disk persistent storage
	private static DiskStorage diskStorage;										// KVServer persistent disk storage
	private static final String dir = "./data";									// KVServer storage directory on disk file system
	private static final String filePreFix = "persistanceDB.properties";		// storage file prefix

	// M2: Distributed server config
	private static boolean distributed;											// flag to record KVServer running in distributed or non-distributed mode
	private static DistributedServerStatus serverStatus;						// status of current running KVServer instance
	private static String serverName;											// KVServer name in the format of ip:port
	private static Map<String, Metadata> serverMetadatasMap;					// metadata for every running distributed KVServer instances
	private static Metadata serverMetadata;										// metadata for current instance of KVServer 
	private static boolean writeLock;											// lock server from KVClient write operation 

	// M2: ZooKeeper config
	private ZooKeeper zk;														// ZooKeeper client instance
	private String zkHostname;													// ZooKeeper service running hostname
    private int zkPort;															// ZooKeeper service listening port
    private static final int zkTimeout = 20000;									// ZooKeeper client timeout
	private static final String zkRootNodePath = "/StorageServerRoot";			// ZooKeeper path to root zNode
	private String zkServerNodePath;											// ZooKeeper path to child (KVServer) zNode

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
		/* M1: non-distributed KVServer data members */
		this.serverSocket = null;
		this.port = port;
		this.cacheSize = cacheSize;
		this.clientThreads = new ArrayList<Thread>();
		this.serverName = getHostname()+":"+getPort();
		if (storageFileExist())
			this.diskStorage = new DiskStorage(filePreFix, serverName);
		else
			this.diskStorage = new DiskStorage(serverName);
		this.distributed = false;
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
		/* M1: non-distributed KVServer data members */
		this.serverSocket = null;
		this.port = Integer.parseInt(serverName.split(":")[1]);	// port is contained in server name
		this.cacheSize = 0;
		this.clientThreads = new ArrayList<Thread>();
		this.serverName = serverName;
		if (storageFileExist())
			this.diskStorage = new DiskStorage(filePreFix, serverName);
		else
			this.diskStorage = new DiskStorage(serverName);
		/* M2: distributed KVServer data members */
		this.distributed = true;
		this.writeLock = false;
		/* M2: ZooKeeper data */
		this.zkServerNodePath = zkRootNodePath+"/"+serverName;
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
		this.serverMetadata = null;
		// creating ZooKeeper client
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
		// creating KVServer ZNode on ECS ZooKeeper server
		try {
			if (zk.exists(zkServerNodePath, false) == null) {
				zk.create(zkServerNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		// setting server metadata and metadata list
		try {
			byte[] kvAdminMsgBytes = zk.getData(zkServerNodePath, new Watcher() {
				// handle hashRing update
				public void process(WatchedEvent we) {
					if (!running) return;
					try {
						byte[] kvAdminMsgBytes = zk.getData(zkServerNodePath, this, null);
						String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
						// System.out.println("#######################################");
						// System.out.println("KVServer getData constructor watcher");
						// System.out.println(kvAdminMsgStr);
						// System.out.println("#######################################");
						processKVAdminMesage(kvAdminMsgStr);
					} catch (KeeperException | InterruptedException e) {
						logger.error(e);
					}
				}
			}, null);
			String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
			// System.out.println("#######################################");
			// System.out.println("KVServer getData constructor");
			// System.out.println(kvAdminMsgStr);
			// System.out.println("#######################################");
			processKVAdminMesage(kvAdminMsgStr);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
	}

	/* M1: Non-distributed KVServer methods */

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

	/* M2: Distributed KVServer methods */

	@Override
	public void start(){
		logger.info("KVServer started, accepting client requests.");
		serverStatus = DistributedServerStatus.START;
	} 

	@Override
	public void stop(){
		logger.info("KVServer stopped, rejecting client requests.");
		serverStatus = DistributedServerStatus.STOP;
	}

	public DistributedServerStatus getServerStatus (){
		return serverStatus;
	}

	@Override
	public void lockWrite(){ 
		logger.info("KVServer acquiring write lock, rejecting client write requests.");
		writeLock = true; 
	}

	@Override
	public void unlockWrite(){ 
		logger.info("KVServer releasing write lock, accepting client write requests.");
		writeLock = false; 
	}

	@Override
	public boolean getWriteLock(){ 
		return writeLock; 
	}

	@Override
	public boolean moveData(String[] hashRange, String targetName) throws Exception{
		// get start and stop big integer value
		BigInteger start = new BigInteger(hashRange[0]);
		BigInteger stop = new BigInteger(hashRange[1]);
		// Entering critical region and acquire write lock
		lockWrite();
		// get out of range KV pairs and remove from disk storage
		Map<String, String> kvOutOfRange = getKVOutOfRange();
		Iterator it = kvOutOfRange.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry kvPair = (Map.Entry)it.next();
			String key = (String) kvPair.getKey(); 
			if (!diskStorage.mdKeyWithinRange(diskStorage.mdKey(key), start, stop)){
                System.out.println("Error: I should not reach here");
				logger.error("Error: I should not reach here");
				kvOutOfRange.remove(key);
            }
			else {
				diskStorage.delelteKV(key);
			}
		}
		// send KVAdminMessage with KV pairs data
		KVAdminMessage sendMsg = new KVAdminMessage(KVAdminType.TRANSFER_KV, null, kvOutOfRange);
		String zkDestServerNodePath = zkRootNodePath + "/" + targetName;
		zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
		// Leaving critical region and releasing write lock
		unlockWrite();
		return true;
	}

	/**
	 * Receive KV messages from KVAdminMessage data transfer and store in disk storage.
	 * 
	 * Note: this function assumes it holds write lock. 
	 * 
	 * @param kvAdminMsgStr KVAdminMessage string
	 */
	public void receiveKVData(String kvAdminMsgStr){
		// TODO: store KV pair into disk storage
		// Create KVAdminMessage from input string
		KVAdminMessage recvMsg = new KVAdminMessage(kvAdminMsgStr);
		KVAdminType recvMsgType = recvMsg.getMessageType();
		// Verify message type is TRANSFER_KV KV data
		if (recvMsgType != KVAdminType.TRANSFER_KV){
			logger.error("Unhandled case: KVAdminMessage type mismatch");
			return;
		}
		// Entering critical region and acquire write lock
		lockWrite();
		// Get KV pairs data and store into disk
		Map<String, String> recvMsgKvData = recvMsg.getMessageKVData();
		Iterator it = recvMsgKvData.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry kvPair = (Map.Entry)it.next();
			this.diskStorage.put(kvPair.getKey().toString(), kvPair.getValue().toString());
		}
		// Leaving critical region and releasing write lock
		unlockWrite();
	}

	/** 
	 * Update metadata list and servermetadata from byte array.
	 * 
	 * @param kvAdminMsgStr KVAdminMessage string
	 */
	public void setMetadata(String kvAdminMsgStr){
		KVAdminMessage recvMsg = new KVAdminMessage(kvAdminMsgStr);
		String msgType = recvMsg.getMessageTypeString();
		if (msgType == "UPDATE"){
			this.serverMetadatasMap = recvMsg.getMessageMetadata();
			this.serverMetadata = serverMetadatasMap.get(serverName);
		}
		// TODO move data to proper KVServers
		// ??? How do we know target name ???
		// ??? is Metadata transferable in string?
		// moveData(hashRange, targetName)
	}

	@Override
	public Metadata getServerMetadata(){
		return serverMetadata;
	}

	@Override
	public Map<String, Metadata> getServerMetadatasMap(){
		return serverMetadatasMap;
	}

	@Override
	public Map<String, String> getKVOutOfRange(){
		return diskStorage.getKVOutOfRange(serverMetadata.start, serverMetadata.stop);
	}

	public boolean distributed(){
		return distributed;
	}

	public void processKVAdminMesage (String kvAdminMsgStr){
		// empty message content
		if (kvAdminMsgStr.equals(""))
			return;
		// create KVAdminMessage and perform actions according to message type
		KVAdminMessage recvMsg = new KVAdminMessage(kvAdminMsgStr);
		switch(recvMsg.getMessageType()){
			case INIT:			// INIT + metadata + null
				if (serverStatus == DistributedServerStatus.STOP)
					setMetadata(kvAdminMsgStr);
				break;
			case START:			// START + null + null
				start();
				break;
			case STOP:			// STOP + null + null
				stop();
				break;
			case UPDATE:		// UPDATE + metadata + null
				setMetadata(kvAdminMsgStr);
				break;
			case SHUTDOWN:		// SHUTDOWN + null + null
				close();
				break;
			case TRANSFER_KV:	// TRANSFER_KV + null + kv-pairs
				receiveKVData(kvAdminMsgStr);
				break;
			default:
		}
	}

	public static void main(String[] args) throws IOException {
		try {
			new LogSetup("logs/server.log", Level.OFF);
			// create non-distributed KVServer instance
			try{
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				KVServer kvServer = new KVServer(port, cacheSize, strategy);
				kvServer.run();
			}
			// create distributed KVServer instance
			catch (NumberFormatException e){
				String serverName = args[0];
				int zkPort = Integer.parseInt(args[1]);
				String zkHostname = args[2];
				KVServer kvServer = new KVServer(serverName, zkPort, zkHostname);
				kvServer.run();
			}
		}
		catch (IOException e) {
			logger.error("Error! Unable to initialize server logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}