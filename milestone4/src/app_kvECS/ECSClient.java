package app_kvECS;

import ecs.ECSConsistantHashRing;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.IECSNode.STATUS;
import logger.LogSetup;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessage.KVAdminType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;

public class ECSClient implements IECSClient{

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS> ";
    private ECSConsistantHashRing hashRingDB;
    private String sourceConfigPath;
    private HashMap<String, IECSNode.STATUS> serverStatusMap = new HashMap<>(); // all servers in conf, string = ip:port
    private ArrayList<String> curServers = new ArrayList<>();    // INUSE + IDLE servers
    private Object ExceptionInInitializerError;
    private boolean stop = false;
    private BufferedReader stdin;
    
    private static final String zkRootNodePath = "/StorageServerRoot";          // ZooKeeper path to root zNode
	private static final String zkRootDataPathPrev = "/StorageServerDataPrev";	// ZooKeeper path to root zNode for data replication on previous node
	private static final String zkRootDataPathNext = "/StorageServerDataNext";  // ZooKeeper path to root zNode for data replication on next node
    // private static final String serverDir = System.getProperty("user.dir");
    private static final String serverDir = "/Users/Zichun.Chong@ibm.com/Desktop/ece419/project/milestone4";
    private static final String serverJar = "m4-server.jar";
    private static final int zkPort = 2181;
    private static final String zkHost = "localhost";
    private static final int zkTimeout = 20000;
    private ZooKeeper zk;

    public ECSClient(String configFilePath){
        sourceConfigPath = configFilePath;
        loadDataFromConfigFile();
    }

    public void ECSInitialization(int count) {
        try{
            hashRingDB = new ECSConsistantHashRing();
            initializeZooKeeper();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    private void initializeZooKeeper(){
        // Create ZooKeeper client instance
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            this.zk = new ZooKeeper(zkHost+":"+zkPort, zkTimeout, new Watcher(){
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected){
                        logger.info("Successfully connected to ZooKeeper Server.");
                        System.out.println("Successfully connected to ZooKeeper Server.");
                        latch.countDown();
                    }
                }
            });
            logger.info("Connecting to ZooKeeper Server ...");
            System.out.println("Connecting to ZooKeeper Server ...");
            latch.await();
        } catch (IOException | InterruptedException e){
            logger.error("Cannot to ZooKeeper Server", e);
        }

        // Create storage server root zNode in ZooKeeper
        try {
            if (zk.exists(zkRootNodePath, false) == null) {
                zk.create(zkRootNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created ZooKeeper Root Node: " + zkRootNodePath);
            }
            if (zk.exists(zkRootDataPathPrev, false) == null) {
                zk.create(zkRootDataPathPrev, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created ZooKeeper Root Node: " + zkRootDataPathPrev);
            }
            if (zk.exists(zkRootDataPathNext, false) == null) {
                zk.create(zkRootDataPathNext, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created ZooKeeper Root Node: " + zkRootDataPathNext);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Cannot create ZooKeeper root node", e);
        }
    }

    private void loadDataFromConfigFile(){
        try (BufferedReader br = new BufferedReader(new FileReader(sourceConfigPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line.
                String[] dataArray = line.split(" ");
                if (dataArray.length<3){
                    logger.error("bad config file format");
                    throw (Throwable) ExceptionInInitializerError;
                }
                serverStatusMap.put(dataArray[1]+":"+dataArray[2], IECSNode.STATUS.OFFLINE);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    public List<String> findAllAvaliableServer(){
        List<String> avaliableServer = new ArrayList<>();
        Iterator<Map.Entry<String,IECSNode.STATUS>> it = serverStatusMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,IECSNode.STATUS> pair = it.next();
            if (pair.getValue() == IECSNode.STATUS.INUSE || pair.getValue() == IECSNode.STATUS.IDLE){
                continue;
            }
            avaliableServer.add(pair.getKey().toString());
        }
        return avaliableServer;
    }

    public List<String> addNodesByName(int count){
        if (curServers.size() == serverStatusMap.size()){
            logger.error("All servers in the configurations are deployed");
            return null;
        }
        Random rand = new Random();
        List<String> avaliableServer = findAllAvaliableServer();
        List<String> addServerName = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int randIndex = rand.nextInt(avaliableServer.size());
            String newServerName = avaliableServer.get(randIndex);
            serverStatusMap.replace(newServerName, IECSNode.STATUS.IDLE);
            avaliableServer.remove(randIndex);
            addServerName.add(newServerName);
        }
        return addServerName;
    }

    @Override
    public boolean start() {
        // Create KVAdminMessage with type START
        KVAdminMessage sendMsg = new KVAdminMessage("ECS", KVAdminType.START, null, null);
        for (String server : curServers){
            // change status to INUSE
            serverStatusMap.put(server, IECSNode.STATUS.INUSE);
            String zkDestServerNodePath = zkRootNodePath + "/" + server;
            try {
                while (zk.exists(zkDestServerNodePath, false) == null){
                    awaitNodes(1, 2000);
                }
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
                logger.info("Sending KVAdmin Message to " + zkDestServerNodePath + ", message content: " + sendMsg.toString());
            } catch (KeeperException | InterruptedException e){
                logger.error("Start KVServer failed", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean stop() {
        // Send KVAdminMessage with type STOP
        KVAdminMessage sendMsg = new KVAdminMessage("ECS", KVAdminType.STOP, null, null);
        for (String server : curServers){
            try {
                serverStatusMap.put(server, IECSNode.STATUS.IDLE);
                String zkDestServerNodePath = zkRootNodePath + "/" + server;
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
                logger.info("Sending KVAdmin Message to " + zkDestServerNodePath + ", message content: " + sendMsg.toString());
            } catch (KeeperException | InterruptedException e){
                logger.error("Stop KVServer failed", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        // Send KVAdminMessage with type SHUTDOWN
        KVAdminMessage sendMsg = new KVAdminMessage("ECS", KVAdminType.SHUTDOWN, null, null);
        for (String server : curServers){
            try {
                serverStatusMap.put(server, IECSNode.STATUS.OFFLINE);
                hashRingDB.removeNodebyServerName(server);
                String zkDestServerNodePath = zkRootNodePath + "/" + server;
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
                logger.info("Sending KVAdmin Message to " + zkDestServerNodePath + ", message content: " + sendMsg.toString());
            } catch (KeeperException | InterruptedException e){
                logger.error("Shutdown KVServer failed", e);
                return false;
            } catch (Throwable e){
                logger.error("Shutdown KVServer failed", e);
                return false;
            }
        }
        curServers.clear();
        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        Random rand = new Random();
        List<String> avaliableServer = findAllAvaliableServer();
        int randIndex = rand.nextInt(avaliableServer.size());
        String newServerName = avaliableServer.get(randIndex);
        
        return addNode(cacheStrategy, cacheSize, newServerName);
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize, String newServerName) {
        serverStatusMap.put(newServerName, IECSNode.STATUS.IDLE);
        curServers.add(newServerName);
        
        ECSNode newNode = null;
        if (hashRingDB.getHashRing().isEmpty()){
            List<String> newServerNameList = new ArrayList<>();
            newServerNameList.add(newServerName);
            try {
                hashRingDB.initializeHashRing(newServerNameList);
                newNode = this.hashRingDB.getECSNodeFromName(newServerName);
            } catch (Throwable e){
                logger.error(e);
            }
        }
        else{
            try {
                newNode = this.hashRingDB.getECSNodeFromName(newServerName);
                this.hashRingDB.addNewNodeByNode(newNode);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }

        // ssh start KVServer instance remotely
        String cdCmd = " cd " + serverDir + "; ";
        String mkdirCmd = " mkdir -p " + serverDir + "/logs; ";
        String startServerCmd = " java -jar " + serverDir + "/" + serverJar + " " + newServerName + " " + zkPort + " "  + zkHost;
        String nohupCmd = " nohup " + startServerCmd + " &> logs/nohup." + newServerName + ".out &";
        String sshStartCmd = "ssh -o StrictHostKeyChecking=no -n " + zkHost + cdCmd + mkdirCmd + nohupCmd;
        try {
            Runtime.getRuntime().exec(sshStartCmd);
            logger.info("Creating KVServer with command: " + sshStartCmd);
        } catch (IOException e) {
            logger.error("Error: cannot ssh start storage server node", e);
            System.out.println("Error: cannot ssh start storage server node");
        }

        awaitNodes(1, 2000);
        setupNodes(curServers.size(), cacheStrategy, cacheSize, null);
        awaitNodes(1, 2000);

        try {
            detectZkNodeCrash(newServerName);
        }
        catch (KeeperException | InterruptedException | IllegalArgumentException e) {
            logger.error("Error adding new node", e);
        }

        return newNode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        
        if (curServers.size() == serverStatusMap.size()){
            logger.error("All servers in the configurations are deployed.");
            System.out.println("All servers in the configurations are deployed.");
            return null;
        }

        if (count > findAllAvaliableServer().size()){
            logger.error("Too many node(s) to be added.");
            System.out.println("Too many node(s) to be added.");
            return null;
        }

        List<IECSNode> newNodes = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            IECSNode newNode = addNode(cacheStrategy, cacheSize);
            newNodes.add(newNode);
        }

        return newNodes;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize, Collection<String> removeNodeName) {

        Collection<String> nodesToSetup = null;
        if (removeNodeName == null){ // node addition
            nodesToSetup = serverStatusMap.keySet();
        }
        else {  // node removal
            nodesToSetup = removeNodeName;
        }

        if (count > nodesToSetup.size())
            return null;

        for (String servername : nodesToSetup){
            KVAdminType admMsgType = KVAdminType.UNDEFINED;
            if (removeNodeName == null){
                if (serverStatusMap.get(servername) == IECSNode.STATUS.OFFLINE)
                    continue;
                admMsgType = KVAdminType.UPDATE;
            }
            else {
                admMsgType = KVAdminType.UPDATE_REMOVE;
            }

            String zkDestServerNodePath = zkRootNodePath + "/" + servername;
            KVAdminMessage sendMsg = new KVAdminMessage("ECS", admMsgType, hashRingDB.getMetadata(), null);
            try {
                while (zk.exists(zkDestServerNodePath, false) == null){
                    awaitNodes(count, 1000);
                }
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
                logger.info("Sending KVAdmin Message to " + zkDestServerNodePath + ", message content: " + sendMsg.toString());
            } catch (KeeperException | InterruptedException e){
                logger.error(e);
            }
        }
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) {
        CountDownLatch latch = new CountDownLatch(timeout);
        boolean ret = true;
        try {
            ret = latch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Await Nodes has been interrupted!");
        }
        return ret;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames, boolean crashDetected) {
        Collection<String> toRemove = new ArrayList<String>(nodeNames);
        for (String server : toRemove){
            removeNode(server, crashDetected);
        }
        return true;
    }

    public boolean removeNode(String nodeName, boolean crashDetected) {
        // Create KVAdminMessage with type UPDATE_REMOVE and metadata
        try {
            serverStatusMap.put(nodeName, IECSNode.STATUS.OFFLINE);
            hashRingDB.removeNodebyServerName(nodeName);
            curServers.remove(nodeName);
        } catch (Throwable e){
            logger.error("Shutdown KVServer failed", e);
            return false;
        }
        
        if (!crashDetected){
            ArrayList<String> nodeNames = new ArrayList<String>();
            nodeNames.add(nodeName);
            setupNodes(1, "NONE", 0, nodeNames); // to-remove
        }
        awaitNodes(1, 2000);
        setupNodes(curServers.size(), "NONE", 0, null); // running
        awaitNodes(1, 2000);

        if (!crashDetected){
            KVAdminMessage sendMsg = new KVAdminMessage("ECS", KVAdminType.SHUTDOWN, null, null);
            try {
                String zkDestServerNodePath = zkRootNodePath + "/" + nodeName;
                if (zk.exists(zkDestServerNodePath, false) != null){
                    zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
                    logger.info("Sending KVAdmin Message to " + zkDestServerNodePath + ", message content: " + sendMsg.toString());
                }
            } catch (KeeperException | InterruptedException e){
                logger.error("Shutdown KVServer failed", e);
                return false;
            }
        }
        return true;
    }

    public List<String> getCurrentServers(){
        return curServers;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return hashRingDB.getHashRing();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return hashRingDB.getHashRing().get(Key);
    }

    public void detectZkNodeCrash(final String zkChildPath) throws KeeperException, InterruptedException, IllegalArgumentException {
        String zkNodePath = zkRootNodePath + "/" + zkChildPath;
        zk.exists(zkNodePath, new Watcher () {
            @Override
            public void process(WatchedEvent event) {
                try {
                    if (event.getType() == EventType.NodeDeleted && serverStatusMap.get(zkChildPath) != STATUS.OFFLINE){
                        logger.info("KVServer disconnection detected");
                        System.out.println("KVServer disconnection detected, creating a replacement node");
                        System.out.print(PROMPT);
                        List<String> removeNode = new ArrayList<String>();
                        removeNode.add(zkChildPath);
                        removeNodes(removeNode, true);
                        awaitNodes(1, 2000);
                        addNode("NONE", 0, zkChildPath);
                    }
                    else {
                        detectZkNodeCrash(zkChildPath);
                    }
                }
                catch (KeeperException | InterruptedException | IllegalArgumentException e) {
                    logger.error(e);
                }
            }
        });
    }

    public void run() throws Exception{
        while(!stop) {
            System.out.print(PROMPT);
            stdin = new BufferedReader(new InputStreamReader(System.in));
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            }
            catch (IOException e) {
                stop = true;
                printError("No response - Application terminated ");
            }
        }
    }

    private void printError(String error){
        System.out.println("Error! " +  error);
    }

    private void handleCommand(String cmdLine) throws Exception {
        String[] tokens = cmdLine.split("\\s+");
        if (tokens[0].equals("addnodes")) {
            if (tokens.length == 2) {
                int count = Integer.parseInt(tokens[1]);
                if (count <= (serverStatusMap.size() - curServers.size())){
                    System.out.println("Adding storage server nodes");
                    addNodes(count, "NONE", 0);
                }
                else{
                    System.out.println("Error: do not have enough nodes in the pool");
                }
            }
            else{
                System.out.println("Error: number of nodes expected");
            }
        } else if (tokens[0].equals("addnode")) {
            System.out.println("Adding storage server node");
            addNode("NONE", 0);
        } else if (tokens[0].equals("start")) {
            System.out.println("Starting all storage servers");
            start();
        } else if (tokens[0].equals("stop")) {
            System.out.println("Stopping down all storage servers");
            stop();
        } else if (tokens[0].equals("shutdown")) {
            System.out.println("Shutting down down all storage servers");
            shutdown();
        } else if (tokens[0].equals("removenode") || tokens[0].equals("removenodes")) {
            System.out.println("Removing node(s)");
            List<String> removeServerList = new ArrayList<>();
            for (int i = 1; i < tokens.length; i++) {
                removeServerList.add(tokens[i]);
            }
            removeNodes(removeServerList, false);
        } else if (tokens[0].equals("status") || tokens[0].equals("serverstatus")) {
            printServerStatus();
        } else if (tokens[0].equals("hashringstatus") || tokens[0].equals("hashring")) {
            printHashRingStatus();
        }else if(tokens[0].equals("loglevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError(level + " is not a valid log level!");
                    logger.error(level + " is not a valid log level!");
                    printPossibleLogLevels();
                } 
                else {
                    logger.info("Log level changed to level: " + level);
                }
            } 
            else {
                printError("Invalid number of parameters!");
                printPossibleLogLevels();
                logger.error("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else if (tokens[0].equals("quit")) {
            System.out.println("Shutdown all storage servers before exiting");
            shutdown();
            stop = true;
            System.out.println("Removing application data and logs");
            String rmCmd = "rm -r " + serverDir + "/data/ " + serverDir + "/logs/";
            Runtime.getRuntime().exec(rmCmd);
            System.out.println("Application exit!");
        } else {
            printError("Unknown command");
            printHelp();
            logger.error("Unknown command: " + tokens[0]);
        }
    }

    public void printServerStatus() {
        System.out.println("Status of all servers loaded from ecs.config");
        Iterator<Map.Entry<String,IECSNode.STATUS>> it = serverStatusMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String,IECSNode.STATUS> pair = it.next();
            String serverName = (String)pair.getKey();
            IECSNode.STATUS serverStatus = (IECSNode.STATUS)pair.getValue();
            String serverStatusStr = "UNDEFINED";
            switch (serverStatus){
                case IDLE:
                    serverStatusStr = "IDLE";
                    break;
                case OFFLINE:
                    serverStatusStr = "OFFLINE";
                    break;
                case INUSE:
                    serverStatusStr = "INUSE";
                    break;
                default:
                    serverStatusStr = "UNDEFINED";
            }
            System.out.println(" * " + serverName + " --> " + serverStatusStr);
        }
    }

    private void printHashRingStatus() {
        System.out.println("Status of current hash ring of active KVServer");
        hashRingDB.printHashRing("Consistent Hash Ring");
    }

    private void printPossibleLogLevels() {
        System.out.println("Possible log levels are:");
        System.out.println(LogSetup.getPossibleLogLevels());
    }

    public void printHelp(){
        System.out.println("External Configuration Service (ECS) Client");
        System.out.println("Possible commands are:");
        System.out.println("    addnode");
        System.out.println("    addnodes <number of nodes>");
        System.out.println("    removenode <list of server names>");
        System.out.println("    start");
        System.out.println("    stop");
        System.out.println("    shutdown");
        System.out.println("    status | serverstatus");
        System.out.println("    hashring | hashringstatus");
        System.out.println("    loglevel <level>");
        System.out.println("    quit");
        System.out.println("    help");
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } 
        else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } 
        else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } 
        else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } 
        else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } 
        else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } 
        else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } 
        else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.OFF);
            if (args.length == 0) {
                args = new String[1];
                args[0] = "ecs.config";
            }
            String configFilePath = args[0];
            ECSClient app = new ECSClient(configFilePath);
            app.ECSInitialization(0);
            app.run();
        }
        catch (IOException e) {
            logger.error("Error! Unable to initialize ECS logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
