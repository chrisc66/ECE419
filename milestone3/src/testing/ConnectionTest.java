package testing;

import app_kvServer.KVServer;
import client.KVStore;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;

public class ConnectionTest extends TestCase {

	// KVServer
	KVServer kvServer;
		
	@Before
	public void setUp() {
		kvServer = new KVServer(50000, 10, "NONE");
		new Thread(kvServer).start();
	}

	@After
	public void tearDown(){
		kvServer.close();
	}

	@Test
	public void testConnectionSuccess() {
		
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 50000);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		kvClient.disconnect();
		assertNull(ex);
	}
	
	@Test
	public void testUnknownHost() {

		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		kvClient.disconnect();
		assertTrue(ex instanceof UnknownHostException);
	}
	
	@Test
	public void testIllegalPort() {

		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		kvClient.disconnect();
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
}
