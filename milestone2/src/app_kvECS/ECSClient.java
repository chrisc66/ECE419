package app_kvECS;

import ecs.ECSConsistantHashRing;
import ecs.ECSNode;
import ecs.ECSUI;
import ecs.IECSNode;
import jdk.internal.net.http.common.Utils.ServerName;
import logger.LogSetup;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessage.KVAdminType;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
    private ECSUI ECSClientUI;
    private ArrayList<String> curServers = new ArrayList<>();    // INUSE + IDLE servers
    private Object ExceptionInInitializerError;
    private boolean stop = false;
    private BufferedReader stdin;
    
    private static final String zkRootNodePath = "/StorageServerRoot";
    // private static final String serverDir = System.getProperty("user.dir");
    private static final String serverDir = "/Users/Zichun.Chong@ibm.com/Desktop/ece419/project/milestone2";
    private static final String serverJar = "m2-server.jar";
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
        // Create ZooKeeper server instance and run in a separate thread
        Runnable zkServerRun = new Runnable(){
            public void run(){
                try{
                    String zkServerConfPath = "zookeeper-3.4.11/conf/zoo.cfg";
                    ServerConfig config = new ServerConfig();
                    config.parse(zkServerConfPath);
                    ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
                    zkServer.runFromConfig(config);
                } catch (ConfigException | IOException | AdminServerException | NoClassDefFoundError e){}
            }
        };
        Thread zkServer = new Thread(zkServerRun);
        zkServer.start();

        // Create ZooKeeper client instance
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            this.zk = new ZooKeeper(zkHost+":"+zkPort, zkTimeout, new Watcher(){
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

        // Create storage server root zNode in ZooKeeper
        try {
            if (zk.exists(zkRootNodePath, false) == null) {
                zk.create(zkRootNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // TODO Maybe this is a good location for getChildren watcher
            zk.getChildren(zkRootNodePath, new Watcher () {
                @Override
                public void process(WatchedEvent event) {
                    // if (event.getType() == EventType.ChildWatchRemoved){
                    //     awaitNodes(1, 50000);
                    //     ArrayList<String> missingChildren = new ArrayList<>();
                    //     for (String server : curServers){
                    //         try {
                    //             String zkChildNodePath = zkRootNodePath + "/" + server;
                    //             if (zk.exists(zkChildNodePath, false) == null){
                    //                 missingChildren.add(server);
                    //             }
                    //         } catch (KeeperException | InterruptedException e){
                    //             logger.error(e);
                    //         }
                    //     }
                    //     removeNodes(missingChildren);
                    // }
                }
            }, null);
        } catch (KeeperException | InterruptedException e) {
            logger.error(e);
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
        Iterator it = serverStatusMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
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
        // KVServers responds to both ECS and KVClient
        // Create KVAdminMessage with type START
        // Send message with zk.setData() to correct KVServer znode
        KVAdminMessage sendMsg = new KVAdminMessage(KVAdminType.START, null, null);
        for (String server : curServers){
            // change status to INUSE
            serverStatusMap.put(server, IECSNode.STATUS.INUSE);
            String zkDestServerNodePath = zkRootNodePath + "/" + server;
            try {
                // System.out.println("#######################################");
                // System.out.println("ECS Client send KVAdminMessage");
                // System.out.println(sendMsg.toString());
                // System.out.println("#######################################");
                while (zk.exists(zkDestServerNodePath, false) == null){
                    awaitNodes(1, 2000);
                }
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
            } catch (KeeperException | InterruptedException e){
                logger.error("Start KVServer failed", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean stop() {
        // KVServers still responds to ECS but not KVClient
        // Create KVAdminMessage with type STOP
        // Send message with zk.setData() to correct KVServer znode
        KVAdminMessage sendMsg = new KVAdminMessage(KVAdminType.STOP, null, null);
        for (String server : curServers){
            try {
                serverStatusMap.put(server, IECSNode.STATUS.IDLE);
                String zkDestServerNodePath = zkRootNodePath + "/" + server;
                // System.out.println("#######################################");
                // System.out.println("ECS Client send KVAdminMessage");
                // System.out.println(sendMsg.toString());
                // System.out.println("#######################################");
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
            } catch (KeeperException | InterruptedException e){
                logger.error("Stop KVServer failed", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        // Stops all server instances and exits the remote processes
        // Create KVAdminMessage with type SHUTDOWN
        // Send message with zk.setData() to correct KVServer znode
        KVAdminMessage sendMsg = new KVAdminMessage(KVAdminType.SHUTDOWN, null, null);
        for (String server : curServers){
            try {
                serverStatusMap.put(server, IECSNode.STATUS.OFFLINE);
                hashRingDB.removeNodebyServerName(server);
                String zkDestServerNodePath = zkRootNodePath + "/" + server;
                // System.out.println("#######################################");
                // System.out.println("ECS Client send KVAdminMessage");
                // System.out.println(sendMsg.toString());
                // System.out.println("#######################################");
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
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
        List<IECSNode> newNode = (List<IECSNode>) addNodes(1,cacheStrategy,cacheSize);
        return newNode.get(0);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // Create KVAdminMessage with type UPDATE and metadata
        // Send message with zk.setData() to correct KVServer znode
        
        // select x number of servers from the avalibale server list
        if (curServers.size() == serverStatusMap.size()){
            logger.error("All servers in the configurations are deployed");
            return null;
        }

        Random rand = new Random();
        List<String> avaliableServer = findAllAvaliableServer();
        List<IECSNode> addServerName = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            int randIndex = rand.nextInt(avaliableServer.size());
            String newServerName = avaliableServer.get(randIndex);
            serverStatusMap.put(newServerName, IECSNode.STATUS.IDLE);
            curServers.add(newServerName);
            avaliableServer.remove(randIndex);

            if (hashRingDB.getHashRing().isEmpty()){
                List<String> newServerNameList = new ArrayList<>();
                newServerNameList.add(newServerName);
                try {
                    hashRingDB.initializeHashRing(newServerNameList);
                    ECSNode newNode = this.hashRingDB.getECSNodeFromName(newServerName);
                    addServerName.add(newNode);
                } catch (Throwable e){
                    logger.error(e);
                }
            }
            else{
                try {
                    ECSNode newNode = this.hashRingDB.getECSNodeFromName(newServerName);
                    addServerName.add(newNode);
                    this.hashRingDB.addNewNodeByNode(newNode);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            }

            // ssh start KVServer instance remotely
            String cdCmd = " cd " + serverDir + "; ";
            String startServerCmd = " java -jar " + serverDir + "/" + serverJar + " " + newServerName + " " + zkPort + " "  + zkHost;
            String nohupCmd = " nohup " + startServerCmd + " &> logs/nohup." + newServerName + ".out &";
            String sshStartCmd = "ssh -o StrictHostKeyChecking=no -n " + zkHost + cdCmd + nohupCmd;
            // System.out.println("#######################################");
            // System.out.println("ECS Client remote start KVServer");
			// System.out.println(sshStartCmd);
			// System.out.println("#######################################");
            try {
                Process p = Runtime.getRuntime().exec(sshStartCmd);
                logger.info("Creating KVServer with command: " + sshStartCmd);
            } catch (IOException e) {
                logger.error("Error: cannot ssh start storage server node", e);
                System.out.println("Error: cannot ssh start storage server node");
            }
        }

        setupNodes(count, cacheStrategy, cacheSize);

        return addServerName;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > serverStatusMap.size()) 
            return null;

        for (String servername : serverStatusMap.keySet()){
            if (serverStatusMap.get(servername) == IECSNode.STATUS.OFFLINE){
                continue;
            }
            String zkDestServerNodePath = zkRootNodePath + "/" + servername;
            KVAdminMessage sendMsg = new KVAdminMessage(KVAdminType.INIT, hashRingDB.getMetadata(), null);
            try {
                // System.out.println("#######################################");
                // System.out.println("ECS Client send KVAdminMessage");
                // System.out.println(sendMsg.toString());
                // System.out.println("#######################################");
                while (zk.exists(zkDestServerNodePath, false) == null){
                    awaitNodes(count, 2000);
                }
                zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
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
    public boolean removeNodes(Collection<String> nodeNames) {
        // Create KVAdminMessage with type UPDATE and metadata
        // Send message with zk.setData() to correct KVServer znode
        for (String server: nodeNames){
            try {
                serverStatusMap.put(server, IECSNode.STATUS.OFFLINE);
                hashRingDB.removeNodebyServerName(server);
                curServers.remove(server);
            } catch (Throwable e){
                logger.error("Shutdown KVServer failed", e);
                return false;
            }
        }
        KVAdminMessage sendMsg = new KVAdminMessage(KVAdminType.SHUTDOWN, null, null);
        for (String server: nodeNames){
            try {
                String zkDestServerNodePath = zkRootNodePath + "/" + server;
                // System.out.println("#######################################");
                // System.out.println("ECS Client send KVAdminMessage");
                // System.out.println(sendMsg.toString());
                // System.out.println("#######################################");
                if (zk.exists(zkDestServerNodePath, false) != null){
                    zk.setData(zkDestServerNodePath, sendMsg.toBytes(), zk.exists(zkDestServerNodePath, false).getVersion());
                }
            } catch (KeeperException | InterruptedException e){
                logger.error("Shutdown KVServer failed", e);
                return false;
            }
        }
        setupNodes(curServers.size(), "NONE", 0);
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return hashRingDB.getHashRing();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return hashRingDB.getHashRing().get(Key);
    }

    public void run() throws Exception{
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
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
        } else if (tokens[0].equals("removenode")) {
            System.out.println("Removing nodes");
            List<String> removeServerList = new ArrayList<>();
            for (int i = 1; i < tokens.length; i++) {
                removeServerList.add(tokens[i]);
            }
            removeNodes(removeServerList);
        } else if (tokens[0].equals("status")) {
            printServerStatus();
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else if (tokens[0].equals("quit")) {
            System.out.println("Shutdown all storage servers before exiting");
            shutdown();
            System.out.println("Removing application data and logs");
            String rmCmd = "rm -r " + serverDir + "/data/ " + serverDir + "/logs/";
            Process p = Runtime.getRuntime().exec(rmCmd);
            System.out.println("Application exit!");
            System.exit(0);
        } else {
            printError("Unknown command");
            printHelp();
            logger.error("Unknown command: " + tokens[0]);
        }
    }

    public void printServerStatus (){
        System.out.println("Status of all servers loaded from ecs.config");
        Iterator it = serverStatusMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
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

    public void printHelp(){
        System.out.println("External Configuration Service (ECS) Client");
        System.out.println("Possible commands are:");
        System.out.println("    addnode");
        System.out.println("    addnode <numer of nodes>");
        System.out.println("    removenode <server names>");
        System.out.println("    start");
        System.out.println("    stop");
        System.out.println("    shutdown");
        System.out.println("    status");
        System.out.println("    quit");
        System.out.println("    help");
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
