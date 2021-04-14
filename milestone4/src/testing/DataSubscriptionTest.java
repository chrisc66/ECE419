package testing;

import app_kvECS.*;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataSubscriptionTest extends TestCase {
    
    // ECS Client
    private static final String configFilePath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecsClient;
    // KVServer
    private static final int numKvServer = 2;
    private static final String cacheStrategy = "NONE";
    private static final int cacheSize = 0;
    // KVClient
    private KVStore kvClient1;
    private KVStore kvClient2;
    private KVStore kvClient3;

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
        
        kvClient1 = new KVStore(hostname, port);
        kvClient2 = new KVStore(hostname, port);
        kvClient3 = new KVStore(hostname, port);
        try {
			kvClient1.connect();
            kvClient2.connect();
            kvClient3.connect();
		} catch (Exception e) {}
    }

    @After
	public void tearDown() {
		kvClient1.disconnect();
        kvClient2.disconnect();
        kvClient3.disconnect();
        ecsClient.shutdown();
	}

    @Test
    public void testMultipleClientsPutAndGet() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;
        KVMessage response5 = null;
        KVMessage response6 = null;

        try {
            kvClient1.put("a", "a");
            kvClient1.put("b", "b");
            kvClient2.put("c", "c");
            kvClient2.put("d", "d");
            kvClient3.put("e", "e");
            kvClient3.put("f", "f");
        } catch (Exception e) {
            ex = e;
        }

        try {
            kvClient1.get("a");
			response1 = kvClient1.recvMessage;

            kvClient2.get("a");
			response2 = kvClient2.recvMessage;

            kvClient3.get("a");
			response3 = kvClient3.recvMessage;

            kvClient1.get("a");
			response4 = kvClient1.recvMessage;

            kvClient2.get("a");
			response5 = kvClient2.recvMessage;

            kvClient3.get("a");
			response6 = kvClient3.recvMessage;
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);
        assertNotNull(response5);
        assertNotNull(response6);
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response1.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response2.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response3.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response4.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response5.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response6.getStatus());
        assertEquals("a", response1.getValue());
        assertEquals("a", response2.getValue());
        assertEquals("a", response3.getValue());
        assertEquals("a", response4.getValue());
        assertEquals("a", response5.getValue());
        assertEquals("a", response6.getValue());
        assertNull(ex);
    }

    @Test
    public void testMultipleClientsSubscribeAll() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;

        try {
            kvClient1.subscribe(true);
            kvClient2.subscribe(true);
            kvClient3.subscribe(true);

            kvClient1.put("a", "a");

            Thread.sleep(1000);
            response1 = kvClient1.recvMessage;
            response2 = kvClient2.recvMessage;
            response3 = kvClient3.recvMessage;
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response2);
        assertNotNull(response3);
        if (response1 != null)
            assertTrue  (KVMessage.StatusType.SUBSCRITION_UPDATE != response1.getStatus());
        assertEquals(KVMessage.StatusType.SUBSCRITION_UPDATE, response2.getStatus());
        assertEquals(KVMessage.StatusType.SUBSCRITION_UPDATE, response3.getStatus());
        assertEquals("a", response2.getKey());
        assertEquals("a", response3.getKey());
        assertEquals("a", response2.getValue());
        assertEquals("a", response3.getValue());
        assertNull(ex);
    }

    @Test
    public void testMultipleClientsSubscribeKeys() {

        Exception ex = null;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;

        try {
            kvClient1.subscribe("a b c");
            kvClient2.subscribe("a b c");
            kvClient3.subscribe("a b c");

            kvClient1.put("a", "a");

            Thread.sleep(1000);
            response1 = kvClient1.recvMessage;
            response2 = kvClient2.recvMessage;
            response3 = kvClient3.recvMessage;
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response2);
        assertNotNull(response3);
        if (response1 != null)
            assertTrue  (KVMessage.StatusType.SUBSCRITION_UPDATE != response1.getStatus());
        assertEquals(KVMessage.StatusType.SUBSCRITION_UPDATE, response2.getStatus());
        assertEquals(KVMessage.StatusType.SUBSCRITION_UPDATE, response3.getStatus());
        assertEquals("a", response2.getKey());
        assertEquals("a", response3.getKey());
        assertEquals("a", response2.getValue());
        assertEquals("a", response3.getValue());
        assertNull(ex);
    }

}
