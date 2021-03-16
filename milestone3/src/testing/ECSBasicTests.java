package testing;

import app_kvECS.*;
import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;

public class ECSBasicTests extends TestCase {
    
    // ECS Client
    private static final String configFilePath = "ecs.config";
    // KVServer
    private static final int numKvServer = 8;
    private static final String cacheStrategy = "NONE";
    private static final int cacheSize = 0;

    private ECSClient ecs;
    private KVStore kvClient;

    public void setUp() {
        try {
            ecs = new ECSClient(configFilePath);
            ecs.ECSInitialization(0);
        	ecs.addNodes(numKvServer, cacheStrategy, cacheSize);
        	try {
            	ecs.awaitNodes(1, 10000);
        	} catch (Exception e) {}
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Test error "+e);
        }
    }

    @Test
    public void testConnectionSuccess() {
		Exception ex = null;

        // List<String> avaliableServers = ecs.findAllAvaliableServer();
        // String avaliableServer = avaliableServers.get(0);
        // String[] tokens = avaliableServer.split(":");
        // String hostname = tokens[0];
        // int port = Integer.parseInt(tokens[1]);

        KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}
		
		assertNull(ex);
        return;
	}

    @Test
    public void testPut() {

        Exception ex = null;
        
        try {
            kvClient.put("a", "a");
            kvClient.put("b", "b");
            kvClient.put("c", "c");
            kvClient.put("x", "x");
            kvClient.put("y", "y");
            kvClient.put("z", "z");
            kvClient.put("1", "1");
            kvClient.put("5", "5");
            kvClient.put("9", "9");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        return;
    }

    @Test
    public void testGet() {

        Exception ex = null;

        try {
            kvClient.get("a");
            kvClient.get("b");
            kvClient.get("c");
            kvClient.get("x");
            kvClient.get("y");
            kvClient.get("z");
            kvClient.get("1");
            kvClient.get("5");
            kvClient.get("9");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        return;
    }

    @Test
    public void testDelete() {

        Exception ex = null;

        try {
            kvClient.put("a", "");
            kvClient.put("b", "");
            kvClient.put("c", "");
            kvClient.put("x", "");
            kvClient.put("y", "");
            kvClient.put("z", "");
            kvClient.put("1", "");
            kvClient.put("5", "");
            kvClient.put("9", "");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        return;
    }

    @Test
    public void testShutdown() {

        Exception ex = null;

        try {
            ecs.shutdown();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        return;

    }

}
