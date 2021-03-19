package testing;

import app_kvServer.KVServer;
import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class InteractionTest extends TestCase {
	
	// KVServer
	KVServer kvServer;
	// KVClient 
	KVStore kvClient;
		
	@Before
	public void setUp() {
		kvServer = new KVServer(50000, 10, "NONE");
		new Thread(kvServer).start();

		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} 
		catch (Exception e) {}
	}

	@After
	public void tearDown(){
		kvClient.disconnect();
		kvServer.close();
	}

	@Test
	public void testPut() {

		Exception ex = null;

		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} 
		catch (Exception e) {
			ex = e;
		}

		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null);
		assertTrue(response != null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() {
		
		Exception ex = null;

		kvClient.disconnect();

		String key = "foo";
		String value = "bar";

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);

		try {
			kvClient.connect();
		} 
		catch (Exception e) {}
	}

	@Test
	public void testUpdate() {
		
		Exception ex = null;
		
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		
		Exception ex = null;
		
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		
		Exception ex = null;
		
		String key = "foo";
		String value = "bar";
		KVMessage response = null;

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		
		Exception ex = null;
		
		String key = "an unset value";
		KVMessage response = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

}
