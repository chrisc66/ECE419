package app_kvECS;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import ecs.ECSUI;
import ecs.ECSConsistantHashRing;
import ecs.ECStoServerComm;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class ECSClient implements IECSClient{

    private static Logger logger = Logger.getRootLogger();
    private ECSConsistantHashRing hashRingDB;
    private ECStoServerComm commModule;
    private String sourceConfigPath;
    private List<String> addrPortServerName = new ArrayList<>();
    private ECSUI ECSClientUI;
    private String[] liveServers;
    private Object ExceptionInInitializerError;

    public ECSClient(String configFilePath){
        sourceConfigPath=configFilePath;
        loadDataFromConfigFile();
        hashRingDB = new ECSConsistantHashRing(addrPortServerName);
        commModule = new ECStoServerComm(); //TODO
    }

    public void ECSInitialization() {
        try{
            hashRingDB.generateHashRingFromServerNameList();
            ECSClientUI = new ECSUI();

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
                addrPortServerName.add(dataArray[1]+":"+dataArray[2]);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

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
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
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
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }


    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            String configFilePath = args[0];
            ECSClient app = new ECSClient(configFilePath);
            app.ECSInitialization();
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
