package testing;

import app_kvECS.*;
import client.KVStore;
import ecs.IECSNode;

import java.util.Collection;
import java.util.List;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ECSConsistentHashRingTest extends TestCase {
    
    // ECS Client
    // private static final String configFilePath = System.getProperty("user.dir") + "/ecs.config";
    private static final String configFilePath = "/Users/Zichun.Chong@ibm.com/Desktop/ece419/project/milestone3/ecs.config";
    private ECSClient ecsClient;
    // KVServer
    private static final int numKvServer = 1;
    private static final String cacheStrategy = "NONE";
    private static final int cacheSize = 0;
    // KVClient
    private KVStore kvClient;

    @Before
    public void setUp() {
        // Start ECS Client
        try {
            ecsClient = new ECSClient(configFilePath);
            ecsClient.ECSInitialization(0);
        	ecsClient.addNodes(numKvServer, cacheStrategy, cacheSize);
        	try {
            	ecsClient.awaitNodes(1, 10000);
        	} catch (Exception e) {}
            ecsClient.start();
        } catch (Exception e) {
            System.out.println("ECS Test error "+e);
        }

        // Start KVClient
        List<String> curServers = ecsClient.getCurrentServers();
        String servername = curServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        System.out.println("ECSConsistentHashRingTest setUp: connecting to " + hostname + ":" + port);
        
        kvClient = new KVStore(hostname, port);
        try {
			kvClient.connect();
		} catch (Exception e) {}
    }

    @After
	public void tearDown() {
		kvClient.disconnect();
        ecsClient.shutdown();
	}

    @Test
    public void testAddNode() {

        Exception ex = null;

        ecsClient.addNode(cacheStrategy, cacheSize);

        int numCurServers = ecsClient.getCurrentServers().size();
        int numNodes = ecsClient.getNodes().size();

        ecsClient.shutdown();

        assertEquals(numCurServers, numNodes);
        assertNull(ex);
    }

    @Test
    public void testAddNodes() {

        ecsClient.addNodes(2, cacheStrategy, cacheSize);
        
        int numCurServers = ecsClient.getCurrentServers().size();
        int numNodes = ecsClient.getNodes().size();

        ecsClient.shutdown();

        assertEquals(numCurServers, numNodes);
    }

    @Test
    public void testAddTooManyNodes() {

        Collection<IECSNode> ret = ecsClient.addNodes(1000, cacheStrategy, cacheSize);

        ecsClient.shutdown();

        assertEquals(ret, null);
    }

    @Test
    public void testRemoveNodes() {
        
        ecsClient.addNodes(4, cacheStrategy, cacheSize);
        
        List<String> curServers = ecsClient.getCurrentServers();
        String servername = curServers.get(0);
        ecsClient.removeNode(servername, false);
        
        int numCurServers = ecsClient.getCurrentServers().size();
        int numNodes = ecsClient.getNodes().size();

        ecsClient.shutdown();

        assertEquals(numCurServers, numNodes);
    }

    @Test
    public void testRemoveAllNodes() {

        ecsClient.addNodes(4, cacheStrategy, cacheSize);

        List<String> curServers = ecsClient.getCurrentServers();
        ecsClient.removeNodes(curServers, false);
        
        int numCurServers = ecsClient.getCurrentServers().size();
        int numNodes = ecsClient.getNodes().size();

        ecsClient.shutdown();

        assertEquals(numCurServers, numNodes);
    }

}
