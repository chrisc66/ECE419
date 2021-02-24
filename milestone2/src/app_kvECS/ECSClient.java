package app_kvECS;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.Iterator;

import ecs.*;
import logger.LogSetup;

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class ECSClient implements IECSClient{

    private static Logger logger = Logger.getRootLogger();
    private ECSConsistantHashRing hashRingDB;
    private String sourceConfigPath;
    private HashMap<String, IECSNode.STATUS> serverStatusMap = new HashMap<>(); // all servers in conf, string = ip:port
    private ECSUI ECSClientUI;
    private List<String> curServers = new ArrayList<>();    // running INUSE
    private Object ExceptionInInitializerError;
    
    private static final String zkRootNodePath = "/StorageServerRoot";
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
            ECSClientUI = new ECSUI();
            hashRingDB = new ECSConsistantHashRing(addNodesByName(count),true);
            initializeZooKeeper();
            // TODO call addNodes
            // addNodes(count, "NONE", 0);
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
                } catch (ConfigException | IOException e){}
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

    private List<String> findAllAvaliableServer(){
        List<String> avaliableServer = new ArrayList<>();
        Iterator it = serverStatusMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            if (pair.getValue() == IECSNode.STATUS.INUSE){
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
            serverStatusMap.replace(newServerName, IECSNode.STATUS.INUSE);
            avaliableServer.remove(randIndex);
            addServerName.add(newServerName);
        }
        return addServerName;
    }

    @Override
    public boolean start() {
        // TODO: start all KVServers 
        // KVServers responds to both ECS and KVClient
        // Create KVAdminMessage with type START
        // Send message with zk.setData() to correct KVServer znode
        return false;
    }

    @Override
    public boolean stop() {
        // TODO: stop all KVServers
        // KVServers still responds to ECS but not KVClient
        // Create KVAdminMessage with type STOP
        // Send message with zk.setData() to correct KVServer znode
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO: shutdown all KVServers
        // Stops all server instances and exits the remote processes
        // Create KVAdminMessage with type SHUTDOWN
        // Send message with zk.setData() to correct KVServer znode
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        List<IECSNode> newNode = (List<IECSNode>) addNodes(1,cacheStrategy,cacheSize);
        return newNode.get(0);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
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
            serverStatusMap.replace(newServerName, IECSNode.STATUS.INUSE);
            avaliableServer.remove(randIndex);
            try {
                ECSNode newNode = this.hashRingDB.getECSNodeFromName(newServerName);
                addServerName.add(newNode);
                this.hashRingDB.addNewNodeByNode(newNode);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
            // ssh start KVServer instance remotely
            String startServer = "java -jar " + serverJar + " " + newServerName + " " + zkPort + " "  + zkHost;
            String sshStartServer = "ssh -o StrictHostKeyChecking=no -n " + zkHost + " nohup " + startServer + " &";
            try {
                Process p = Runtime.getRuntime().exec(sshStartServer);
                logger.info("Creating KVServer with command: " + sshStartServer);
            } catch (IOException e) {
                logger.error(e);
                // TODO remove node
            }
        }
        return addServerName;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > serverStatusMap.size()) 
            return null;
        // TODO
        // Create KVAdminMessage with type INIT and metadata
        // Send message with zk.setData() to correct KVServer znode

        // TODO: Maybe this is a good location for watching child nodes
        // for (Map.Entry<String,IECSNode.STATUS> serverEntry : serverStatusMap.entrySet()){
        //     String serverName = serverEntry.getKey();
        //     try {
        //         zk.getChildren(zkRootNodePath+"/"+serverName, new Watcher () {
        //             @Override
        //             public void process(WatchedEvent event) {
        //                 // TODO: check if any child znode is missing
        //             }
        //         }, null);
        //     } catch (KeeperException | InterruptedException e) {
        //         logger.error(e);
        //     }
        // }
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO: wait until all nodes response (how?) or timeout
        
        // TODO: Maybe this is a good location for watching child nodes
		// for (Map.Entry<String,IECSNode.STATUS> serverEntry : serverStatusMap.entrySet()){
        //     String serverName = serverEntry.getKey();
        //     try {
        //         zk.getChildren(zkRootNodePath+"/"+serverName, new Watcher () {
        //             @Override
        //             public void process(WatchedEvent event) {
        //                 // TODO: check if any child znode is missing
        //             }
        //         }, null);
        //     } catch (KeeperException | InterruptedException e) {
        //         logger.error(e);
        //     }
        // }
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        // Create KVAdminMessage with type UPDATE and metadata
        // Send message with zk.setData() to correct KVServer znode
        
        for (String i: nodeNames){
            try {
                hashRingDB.removeNodebyServerName(i);
                /** TODO: needs to notice server with SSH here as well * */
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                return false;
            }
        }
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


    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            String configFilePath = args[0];
            ECSClient app = new ECSClient(configFilePath);
            app.ECSInitialization(4);
            app.ECSClientUI.run();
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
