package testing;

import app_kvECS.*;
import client.KVStore;
import ecs.IECSNode;

import java.util.Collection;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

public class ECSCornerCasesTest extends TestCase {
    
    // ECS Client
    private static final String configFilePath = System.getProperty("user.dir") + "/ecs.config";
    // KVServer
    private static final String cacheStrategy = "NONE";
    private static final int cacheSize = 0;

    @Test
    public void testOneNode() {
        
        Exception ex = null;

        ECSClient ecsClient = null;

        // Start ECS Client
        try {
            ecsClient = new ECSClient(configFilePath);
            ecsClient.ECSInitialization(0);
        	ecsClient.addNodes(1, cacheStrategy, cacheSize);
            ecsClient.awaitNodes(1, 5000);
        } catch (Exception e) {
            ex = e;
        }

        // Start KVClient
        List<String> curServers = ecsClient.getCurrentServers();
        String servername = curServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        System.out.println("ECSCornerCasesTest testOneNode: connecting to " + hostname + ":" + port);
        
        KVStore kvClient = new KVStore(hostname, port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        ecsClient.shutdown();

        assertNull(ex);
    }

    @Test
    public void testTwoNodes() {
        
        Exception ex = null;

        ECSClient ecsClient = null;

        // Start ECS Client
        try {
            ecsClient = new ECSClient(configFilePath);
            ecsClient.ECSInitialization(0);
        	ecsClient.addNodes(2, cacheStrategy, cacheSize);
            ecsClient.awaitNodes(1, 5000);
        } catch (Exception e) {
            ex = e;
        }

        // Start KVClient
        List<String> curServers = ecsClient.getCurrentServers();
        String servername = curServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        System.out.println("ECSCornerCasesTest testTwoNodes: connecting to " + hostname + ":" + port);
        
        KVStore kvClient = new KVStore(hostname, port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        ecsClient.shutdown();

        assertNull(ex);
    }

    @Test
    public void testThreeNodes() {
        
        Exception ex = null;

        ECSClient ecsClient = null;

        // Start ECS Client
        try {
            ecsClient = new ECSClient(configFilePath);
            ecsClient.ECSInitialization(0);
        	ecsClient.addNodes(3, cacheStrategy, cacheSize);
            ecsClient.awaitNodes(1, 5000);
        } catch (Exception e) {
            ex = e;
        }

        // Start KVClient
        List<String> curServers = ecsClient.getCurrentServers();
        String servername = curServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        System.out.println("ECSCornerCasesTest testThreeNodes: connecting to " + hostname + ":" + port);
        
        KVStore kvClient = new KVStore(hostname, port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        ecsClient.shutdown();

        assertNull(ex);
    }

    @Test
    public void testTooManyNodes() {
        
        Exception ex = null;
        Collection<IECSNode> res = null;

        ECSClient ecsClient = null;

        // Start ECS Client
        try {
            ecsClient = new ECSClient(configFilePath);
            ecsClient.ECSInitialization(0);
        	res = ecsClient.addNodes(1000, cacheStrategy, cacheSize);
            ecsClient.awaitNodes(1, 5000);
        } catch (Exception e) {
            ex = e;
        }

        ecsClient.shutdown();
        
        assertNull(res);
        assertNull(ex);
    }

}
