package app_kvECS;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import ecs.*;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class ECSClient implements IECSClient{

    private static Logger logger = Logger.getRootLogger();
    private ECSConsistantHashRing hashRingDB;
    private ECStoServerComm commModule;
    private String sourceConfigPath;
    private HashMap<String, IECSNode.STATUS> serverStatusMap = new HashMap<>();
    private ECSUI ECSClientUI;
    private List<String> curServers = new ArrayList<>();
    private Object ExceptionInInitializerError;

    public ECSClient(String configFilePath){
        sourceConfigPath=configFilePath;
        loadDataFromConfigFile();
        commModule = new ECStoServerComm(); //TODO
    }

    public void ECSInitialization(int count) {
        try{

            ECSClientUI = new ECSUI();
            hashRingDB = new ECSConsistantHashRing(addNodesByName(count),true);

        } catch (Throwable throwable) {
            throwable.printStackTrace();
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
                serverStatusMap.put(dataArray[1]+":"+dataArray[2], IECSNode.STATUS.OFFLine);
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
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        List<IECSNode> newNode = (List<IECSNode>) addNodes(1,cacheStrategy,cacheSize);
        return newNode.get(0);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO

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
                /** TODO: needs to initialize server with SSH here as well * */

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }

        return addServerName;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
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
        // TODO
        return hashRingDB.getHashRing();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
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
