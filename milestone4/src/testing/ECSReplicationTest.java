package testing;

import app_kvECS.*;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ECSReplicationTest extends TestCase {
    
    // ECS Client
    // private static final String configFilePath = System.getProperty("user.dir") + "/ecs.config";
    private static final String configFilePath = "/Users/Zichun.Chong@ibm.com/Desktop/ece419/project/milestone4/ecs.config";
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
            System.out.println("ECS Test error " + e);
        }

        // Start KVClient
        List<String> curServers = ecsClient.getCurrentServers();
        String servername = curServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        System.out.println("ECSReplicationTest setUp: connecting to " + hostname + ":" + port);
        
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
    public void testPut() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        KVMessage response5 = null;

        try {
            response1 = kvClient.put("a", "a");
            response2 = kvClient.put("b", "b");
            response3 = kvClient.put("c", "c");
            response4 = kvClient.put("d", "d");
            response5 = kvClient.put("e", "e");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);
        assertNotNull(response5);
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response1.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response2.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response3.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response4.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response5.getStatus());
        assertNull(ex);
    }

    @Test
    public void testGet() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        KVMessage response5 = null;

        try {
            kvClient.put("a", "a");
            kvClient.put("b", "b");
            kvClient.put("c", "c");
            kvClient.put("d", "d");
            kvClient.put("e", "e");
        } catch (Exception e) {
            ex = e;
        }

        try {
            response1 = kvClient.get("a");
            response2 = kvClient.get("b");
            response3 = kvClient.get("c");
            response4 = kvClient.get("d");
            response5 = kvClient.get("e");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);
        assertNotNull(response5);
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response1.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response2.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response3.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response4.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response5.getStatus());
        assertNull(ex);
    }

    @Test
    public void testDelete() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        KVMessage response5 = null;

        try {
            kvClient.put("a", "a");
            kvClient.put("b", "b");
            kvClient.put("c", "c");
            kvClient.put("d", "d");
            kvClient.put("e", "e");
        } catch (Exception e) {
            ex = e;
        }
        
        try {
            response1 = kvClient.put("a", "");
            response2 = kvClient.put("b", "");
            response3 = kvClient.put("c", "");
            response4 = kvClient.put("d", "");
            response5 = kvClient.put("e", "");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);
        assertNotNull(response5);
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response1.getStatus());
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response2.getStatus());
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response3.getStatus());
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response4.getStatus());
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response5.getStatus());
        assertNull(ex);
    }

    @Test
    public void testGetAfterDelete() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        KVMessage response5 = null;

        try {
            kvClient.put("a", "");
            kvClient.put("b", "");
            kvClient.put("c", "");
            kvClient.put("d", "");
            kvClient.put("e", "");
        } catch (Exception e) {
            ex = e;
        }

        try {
            response1 = kvClient.get("a");
            response2 = kvClient.get("b");
            response3 = kvClient.get("c");
            response4 = kvClient.get("d");
            response5 = kvClient.get("e");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);
        assertNotNull(response5);
        assertEquals(KVMessage.StatusType.GET_ERROR, response1.getStatus());
        assertEquals(KVMessage.StatusType.GET_ERROR, response2.getStatus());
        assertEquals(KVMessage.StatusType.GET_ERROR, response3.getStatus());
        assertEquals(KVMessage.StatusType.GET_ERROR, response4.getStatus());
        assertEquals(KVMessage.StatusType.GET_ERROR, response5.getStatus());
        assertNull(ex);
    }

}
