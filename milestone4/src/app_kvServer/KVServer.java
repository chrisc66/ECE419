package app_kvServer;

import shared.communication.KVCommunicationServer;
import shared.messages.KVAdminMessage;
import shared.messages.Metadata;
import shared.messages.KVAdminMessage.KVAdminType;
import logger.LogSetup;
import DiskStorage.DiskStorage;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.net.*;
import java.io.IOException;
import java.math.BigInteger;
import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
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
	private DiskStorage diskStorage;											// KVServer persistent disk storage
	private static final String dir = "./data";									// KVServer storage directory on disk file system
	private static final String filePreFix = "persistanceDB.properties";		// storage file prefix

	// M2: Distributed server config
	private boolean distributed;											// flag to record KVServer running in distributed or non-distributed mode
	private DistributedServerStatus serverStatus;							// status of current running KVServer instance
	private String serverName;												// KVServer name in the format of ip:port
	private String serverNameHash;											// KVServer name in the format of ip:port
	private Map<String, Metadata> serverMetadatasMap;						// metadata for every running distributed KVServer instances
	private Metadata serverMetadata;										// metadata for current instance of KVServer 
	private Metadata oldServerMetadata;										// metadata for older instance of KVServer 
	private boolean writeLock;												// lock server from KVClient write operation 

	// M2: ZooKeeper config
	private ZooKeeper zk;														// ZooKeeper client instance
	private String zkHostname;													// ZooKeeper service running hostname
    private int zkPort;															// ZooKeeper service listening port
    private static final int zkTimeout = 20000;									// ZooKeeper client timeout
	private static final String zkRootNodePath = "/StorageServerRoot";			// ZooKeeper path to root zNode
	private String zkServerNodePath;											// ZooKeeper path to child (KVServer) zNode
	
	// M3: Data Replication Zookeeper config
	private static final String zkRootDataPathPrev = "/StorageServerDataPrev";	// ZooKeeper path to root zNode for data replication
	private String zkServerDataPathPrev;										// ZooKeeper path to child (KVServer) zNode
	private static final String zkRootDataPathNext = "/StorageServerDataNext";	// ZooKeeper path to root zNode for data replication
	private String zkServerDataPathNext;										// ZooKeeper path to child (KVServer) zNode

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
		this.serverMetadatasMap = new HashMap<>();
		this.serverNameHash = diskStorage.mdKey(serverName).toString();
		/* M2: ZooKeeper data */
		this.zkServerNodePath = zkRootNodePath+"/"+serverName;
		this.zkServerDataPathPrev = zkRootDataPathPrev+"/"+serverName;
		this.zkServerDataPathNext = zkRootDataPathNext+"/"+serverName;
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
		this.serverMetadata = null;
		this.serverMetadata = null;
		// creating ZooKeeper client
		try{
            final CountDownLatch latch = new CountDownLatch(1);
			this.zk = new ZooKeeper(this.zkHostname+":"+this.zkPort, zkTimeout, new Watcher(){
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
			if (zk.exists(zkServerDataPathPrev, false) == null) {
				zk.create(zkServerDataPathPrev, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
			if (zk.exists(zkServerDataPathNext, false) == null) {
				zk.create(zkServerDataPathNext, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		// setup watcher for main server znode 
		try {
			byte[] kvAdminMsgBytes = zk.getData(zkServerNodePath, new Watcher() {
				// handle hashRing update
				public void process(WatchedEvent we) {
					if (!running) return;
					try {
						byte[] kvAdminMsgBytes = zk.getData(zkServerNodePath, this, null);
						String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
						processKVAdminMesage(kvAdminMsgStr);
						KVAdminMessage msg = new KVAdminMessage(kvAdminMsgStr);
						if (msg.getMessageType() == KVAdminType.TRANSFER_KV){
							replicateData();
						}
					} catch (KeeperException | InterruptedException e) {
						logger.error(e);
					}
				}
			}, null);
			String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
			processKVAdminMesage(kvAdminMsgStr);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		// setup watcher for server data znode for data replication on the previous node
		try {
			byte[] kvAdminMsgBytes = zk.getData(zkServerDataPathPrev, new Watcher() {
				// handle hashRing update
				public void process(WatchedEvent we) {
					if (!running) return;
					try {
						byte[] kvAdminMsgBytes = zk.getData(zkServerDataPathPrev, this, null);
						String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
						processKVAdminMesage(kvAdminMsgStr);
					} catch (KeeperException | InterruptedException e) {
						logger.error(e);
					}
				}
			}, null);
			String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
			processKVAdminMesage(kvAdminMsgStr);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
		// setup watcher for server data znode for data replication on the next node
		try {
			byte[] kvAdminMsgBytes = zk.getData(zkServerDataPathNext, new Watcher() {
				// handle hashRing update
				public void process(WatchedEvent we) {
					if (!running) return;
					try {
						byte[] kvAdminMsgBytes = zk.getData(zkServerDataPathNext, this, null);
						String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
						processKVAdminMesage(kvAdminMsgStr);
					} catch (KeeperException | InterruptedException e) {
						logger.error(e);
					}
				}
			}, null);
			String kvAdminMsgStr = new String(kvAdminMsgBytes, StandardCharsets.UTF_8);
			processKVAdminMesage(kvAdminMsgStr);
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
		}
	}

	/* M1: Non-distributed KVServer methods */

	private boolean storageFileExist(){
		File dirFIle = new File(dir);
		if (dirFIle.exists()){
			File dummyStorageFile = new File(dir+'/'+filePreFix+"."+serverName);
			return dummyStorageFile.exists();
		}
		return false;
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
					logger.error("Error! Unable to establish connection. \n");
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
		serverStatus = DistributedServerStatus.SHUTDOWN;
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
		// writeLock ++; 
		writeLock = true;
	}

	@Override
	public void unlockWrite(){ 
		logger.info("KVServer releasing write lock, accepting client write requests.");
		// writeLock --; 
		writeLock = false;
	}

	@Override
	public boolean getWriteLock(){ 
		// return (writeLock == 0); 
		return writeLock;
	}

	@Override
	public boolean moveData(boolean removeNode){
		
		// get out of range KV pairs and remove from disk storage
		Map<String, String> kvOutofRange = null;

		if (removeNode){
			BigInteger start = serverMetadata.start;
			BigInteger stop = serverMetadata.stop;
			kvOutofRange = getKVWithinRange(start, stop);
		}
		else if (oldServerMetadata != null && !oldServerMetadata.stop.equals(serverMetadata.stop)){
			logger.info("Move Data: addition hash ring changed");
			BigInteger stop = serverMetadata.stop;
			BigInteger nextStop = serverMetadatasMap.get(stop.toString()).stop;
			kvOutofRange = getKVWithinRange(stop, nextStop);
		}

		logger.info("Move data, printing all KV, " + diskStorage.getAllKV());
		logger.info("Move data, printing KV within range, " + getKVWithinRange());
		logger.info("Move data, printing KV within range, " + kvOutofRange);
		
		// Entering critical region and acquire write lock
		lockWrite();
		if (removeNode){
			diskStorage.clearDisk();
		}
		else {
			Iterator<Map.Entry<String, String>> it = diskStorage.getKVOutOfRange(serverMetadata.start, serverMetadata.stop).entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> kvPair = it.next();
				String key = (String) kvPair.getKey();
				diskStorage.delelteKV(key);
			}
		}

		if (kvOutofRange == null || kvOutofRange.isEmpty()){
			logger.info("Move Data: empty kvOutofRange, return");
			unlockWrite();
			return true;
		}

		// send KVAdminMessage with KV pairs data
		Metadata targetMetadata = null;
		if (removeNode){
			targetMetadata = serverMetadatasMap.get(serverMetadata.prev.toString());
		}
		else {
			targetMetadata = serverMetadatasMap.get(serverMetadata.stop.toString());
		}

		String targetName = zkRootNodePath + "/" + targetMetadata.serverAddress + ":" + targetMetadata.port;
		try {
			KVAdminMessage sendMsg = new KVAdminMessage(serverName, KVAdminType.TRANSFER_KV, null, kvOutofRange);
			logger.info("Move Data: Sending KVAdmin Message to " + targetName);
			logger.info("Move Data: Message content: " + sendMsg.toString());
			zk.setData(targetName, sendMsg.toBytes(), zk.exists(targetName, false).getVersion());
		} catch (InterruptedException | KeeperException e){
			logger.error("Error occured in moveData", e);
		}

		// Leaving critical region and releasing write lock
		unlockWrite();
		return true;
	}

	/**
	 * This function replicates all key-value pair that is within the hash range of KVServer.
	 * All kv pairs within range are sent to two neighbouring KVServers. 
	 * @return always return true. 
	 */
	public boolean replicateData(){
		
		// get out of range KV pairs and remove from disk storage
		Map<String, String> kvWithinRange = getKVWithinRange();
		logger.info("Replicate data, printing KV within range, " + kvWithinRange);
		logger.info("Replicate data, printing all KV, " + diskStorage.getAllKV());

		if (kvWithinRange.isEmpty()){
			logger.info("Replicate Data: empty kvOutofRange, return");
			return true;
		}

		if (serverMetadatasMap.size() >= 2){
			// Entering critical region and acquire write lock
			lockWrite();
			// send KVAdminMessage with KV pairs data
			BigInteger prev = serverMetadata.prev;
			Metadata targetMetadata = serverMetadatasMap.get(prev.toString());
			// send to previous KVServer
			String targetName = zkRootDataPathPrev + "/" + targetMetadata.serverAddress + ":" + targetMetadata.port;
			try {
				KVAdminMessage sendMsg = new KVAdminMessage(serverName, KVAdminType.TRANSFER_KV, null, kvWithinRange);
				logger.info("Replicate Data 1: Sending KVAdmin Message to " + targetName);
				logger.info("Replicate Data 1: Message content: " + sendMsg.toString());
				zk.setData(targetName, sendMsg.toBytes(), zk.exists(targetName, false).getVersion());
			} catch (InterruptedException | KeeperException e){
				logger.error("Error occured in replicateData", e);
			}
			// Leaving critical region and releasing write lock
			unlockWrite();
		}
		
		if (serverMetadatasMap.size() >= 3){
			// Entering critical region and acquire write lock
			lockWrite();
			// send KVAdminMessage with KV pairs data
			BigInteger stop = serverMetadata.stop;
			Metadata targetMetadata = serverMetadatasMap.get(stop.toString());
			// send to next KVServer
			String targetName = zkRootDataPathNext + "/" + targetMetadata.serverAddress + ":" + targetMetadata.port;
			try {
				KVAdminMessage sendMsg = new KVAdminMessage(serverName, KVAdminType.TRANSFER_KV, null, kvWithinRange);
				logger.info("Replicate Data 2: Sending KVAdmin Message to " + targetName);
				logger.info("Replicate Data 2: Message content: " + sendMsg.toString());
				zk.setData(targetName, sendMsg.toBytes(), zk.exists(targetName, false).getVersion());
			} catch (InterruptedException | KeeperException e){
				logger.error("Error occured in replicateData", e);
			}
			// Leaving critical region and releasing write lock
			unlockWrite();
		} 

		return true;
	}

	/**
	 * This function replicates one key-value pair upon KVServer receives PUT requests 
	 * from KVClient. Only one key-value pair is transferred and sent to two neighbouring 
	 * KVServers.
	 * If value is non-empty string, perform insert / update operation
	 * If value is empty string, perform delete operation 
	 * @param key Key to replicate.
	 * @param Value Value to replicate. 
	 * @return always return true. 
	 */
	public boolean replicateOneKvPair(String key, String value){
		
		// get out of range KV pairs and remove from disk storage
		Map<String, String> kvUpdate = new HashMap<>();
		kvUpdate.put(key, value);

		if (serverMetadatasMap.size() >= 2){
			// Entering critical region and acquire write lock
			lockWrite();
			// send KVAdminMessage with KV pairs data
			BigInteger prev = serverMetadata.prev;
			Metadata targetMetadata = serverMetadatasMap.get(prev.toString());
			// send to previous KVServer
			String targetName = zkRootDataPathPrev + "/" + targetMetadata.serverAddress + ":" + targetMetadata.port;
			try {
				KVAdminMessage sendMsg = new KVAdminMessage(serverName, KVAdminType.TRANSFER_KV, null, kvUpdate);
				logger.info("Replicate One KV Pair 1: Sending KVAdmin Message to " + targetName);
				logger.info("Replicate One KV Pair 1: Message content: " + sendMsg.toString());
				zk.setData(targetName, sendMsg.toBytes(), zk.exists(targetName, false).getVersion());
			} catch (InterruptedException | KeeperException e){
				logger.error("Error occured in replicateData", e);
			}
			// Leaving critical region and releasing write lock
			unlockWrite();
		}
		
		if (serverMetadatasMap.size() >= 3){
			// Entering critical region and acquire write lock
			lockWrite();
			// send KVAdminMessage with KV pairs data
			BigInteger stop = serverMetadata.stop;
			Metadata targetMetadata = serverMetadatasMap.get(stop.toString());
			// send to next KVServer
			String targetName = zkRootDataPathNext + "/" + targetMetadata.serverAddress + ":" + targetMetadata.port;
			try {
				KVAdminMessage sendMsg = new KVAdminMessage(serverName, KVAdminType.TRANSFER_KV, null, kvUpdate);
				logger.info("Replicate One KV Pair 2: Sending KVAdmin Message to " + targetName);
				logger.info("Replicate One KV Pair 2: Message content: " + sendMsg.toString());
				zk.setData(targetName, sendMsg.toBytes(), zk.exists(targetName, false).getVersion());
			} catch (InterruptedException | KeeperException e){
				logger.error("Error occured in replicateData", e);
			}
			// Leaving critical region and releasing write lock
			unlockWrite();
		} 

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

		// Create KVAdminMessage from input string
		KVAdminMessage recvMsg = new KVAdminMessage(kvAdminMsgStr);
		KVAdminType recvMsgType = recvMsg.getMessageType();

		// Verify message type is TRANSFER_KV KV data
		if (recvMsgType != KVAdminType.TRANSFER_KV){
			logger.error("Unhandled case: KVAdminMessage type mismatch");
			return;
		}
		// Verify message source should not be ECS
		if (recvMsg.fromECS()){
			logger.error("Unhandled case: KVAdminMessage source from ECSClient in KV_TRANSFER message");
			return;
		}

		// Entering critical region and acquire write lock
		lockWrite();
		// Get KV pairs data and store into disk
		Map<String, String> recvMsgKvData = recvMsg.getMessageKVData();
		Iterator<Map.Entry<String, String>> it = recvMsgKvData.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> kvPair = it.next();
			if (kvPair.getValue().toString().equals("")){
				diskStorage.delelteKV(kvPair.getKey());
			}
			else {
				diskStorage.put(kvPair.getKey().toString(), kvPair.getValue().toString());
			}
		}
		// Leaving critical region and releasing write lock
		unlockWrite();
		
		// Send ACK back to original KVServer if only one pair 
		if (recvMsgKvData.size() == 1) {
			try {
				String recvMsgSrc = recvMsg.getMessageSource();
				KVAdminMessage sendMsg = new KVAdminMessage(serverName, KVAdminType.ACK_TRANSFER, null, null);
				String targetName = zkRootDataPathNext + "/" + recvMsgSrc;
				logger.info("Replicate One KV Pair 2: Sending KVAdmin Message to " + targetName);
				logger.info("Replicate One KV Pair 2: Message content: " + sendMsg.toString());
				zk.setData(targetName, sendMsg.toBytes(), zk.exists(targetName, false).getVersion());
			} catch (InterruptedException | KeeperException e){
				logger.error("Error occured in replicateData", e);
			}
		}
	}

	/** 
	 * Update metadata list and servermetadata from byte array.
	 * 
	 * @param kvAdminMsgStr KVAdminMessage string
	 */
	public void setMetadata(String kvAdminMsgStr, boolean removeNode){
		if (!removeNode){
			KVAdminMessage recvMsg = new KVAdminMessage(kvAdminMsgStr);
			this.serverMetadatasMap = recvMsg.getMessageMetadata();
			this.oldServerMetadata = serverMetadata;
			this.serverMetadata = serverMetadatasMap.get(serverNameHash);
		}
		
		// move data to proper KVServers
		awaitNode(1000);
		moveData(removeNode);
		awaitNode(1000);
		replicateData();
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

	public Map<String, String> getKVOutOfRange(BigInteger start, BigInteger stop){
		return diskStorage.getKVOutOfRange(start, stop);
	}

	public Map<String, String> getKVWithinRange(){
		return diskStorage.getKVWithinRange(serverMetadata.start, serverMetadata.stop);
	}
	
	public Map<String, String> getKVWithinRange(BigInteger start, BigInteger stop){
		return diskStorage.getKVWithinRange(start, stop);
	}

	public boolean distributed(){
		return distributed;
	}

	public void processKVAdminMesage (String kvAdminMsgStr) throws KeeperException, InterruptedException{
		// empty message content
		if (kvAdminMsgStr.equals(""))
			return;
		// create KVAdminMessage and perform actions according to message type
		KVAdminMessage recvMsg = new KVAdminMessage(kvAdminMsgStr);
		logger.info("Received KVAdmin Message, message content: " + kvAdminMsgStr);
		switch(recvMsg.getMessageType()){
			case START:			// START + null + null
				start();
				break;
			case STOP:			// STOP + null + null
				stop();
				break;
			case UPDATE:		// UPDATE + metadata + null
				setMetadata(kvAdminMsgStr, false);
				break;
			case UPDATE_REMOVE:	// UPDATE_REMOVE + metadata + null
				setMetadata(kvAdminMsgStr, true);
				break;
			case SHUTDOWN:		// SHUTDOWN + null + null
				diskStorage.clearDisk();
				close();
				break;
			case TRANSFER_KV:	// TRANSFER_KV + null + kv-pairs
				System.out.println(kvAdminMsgStr);
				receiveKVData(kvAdminMsgStr);
				break;
			default:
		}
	}

	public boolean awaitNode(int timeout) {
        CountDownLatch latch = new CountDownLatch(timeout);
        boolean ret = false;
        try {
            ret = latch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Await Nodes has been interrupted!");
        }
        return ret;
    }

	public static void main(String[] args) throws IOException {
		// create non-distributed KVServer instance
		try{
			int port = Integer.parseInt(args[0]);
			int cacheSize = Integer.parseInt(args[1]);
			String logString = "logs/server." + args[0] + ".log";
			new LogSetup(logString, Level.INFO);
			String strategy = args[2];
			KVServer kvServer = new KVServer(port, cacheSize, strategy);
			kvServer.run();
			System.exit(0);
		}
		// create distributed KVServer instance
		catch (NumberFormatException e){
			String serverName = args[0];
			int zkPort = Integer.parseInt(args[1]);
			String zkHostname = args[2];
			String logString = "logs/server." + serverName + ".log";
			new LogSetup(logString, Level.INFO);
			KVServer kvServer = new KVServer(serverName, zkPort, zkHostname);
			kvServer.run();
			System.exit(0);
		}
		catch (IOException e) {
			logger.error("Error! Unable to initialize server logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}