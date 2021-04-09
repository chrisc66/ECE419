package testing;

import DiskStorage.DiskStorage;
import app_kvServer.KVServer;
import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class AdditionalTest extends TestCase {
	
	// KVServer
	KVServer kvServer;
	// KVClient 
	KVStore kvClient;
		
	@Before
	public void setUp() {
		kvServer = new KVServer(50000, 10, "NONE");
		new Thread(kvServer).start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {}

		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} 
		catch (Exception e) {}
	}

	@After
	public void tearDown(){
		kvClient.disconnect();
		kvServer.clearCache();
		kvServer.clearStorage();
		kvServer.close();
	}
	
	@Test
	public void testDiskStorageSequence() {
		String keyTest = "dummy";
		String keyTest_1 = "Dummy_11";
		String valTest = "dddddddddddd";

		String valTest_1 = "mmmmmmm";

		DiskStorage DB = new DiskStorage("ip:port");

		DB.put(keyTest, valTest);
		String Val = DB.get(keyTest);
		assertEquals(valTest,Val);
		assertEquals(null,DB.get("dddd"));

		DB.put(keyTest,valTest_1);
		assertEquals(valTest_1,DB.get(keyTest));

		DB.put(keyTest_1,valTest_1);
		DB.delelteKV(keyTest);
		assertEquals(null,DB.get(keyTest));
		assertEquals(valTest_1,DB.get(keyTest_1));

		DB.clearDisk();
		assertEquals(null,DB.get(keyTest));
		assertEquals(null,DB.get(keyTest_1));
	}

	@Test
	public void testDisckStorageBasic(){
		String keyTest = "dummy";
		String valTest = "dddddddddddd";

		DiskStorage DB = new DiskStorage("ip:port");
		try {
			DB.put(keyTest, valTest);
		} catch (Exception e){}
		String Val = DB.get(keyTest);
		assertTrue(valTest==Val);
	}

	@Test
	public void testMultiClients() {

		Exception ex1 = null;
		Exception ex2 = null;
		final int NUM_CLIENTS = 10;

		KVStore clients[] = new KVStore[NUM_CLIENTS];
        Thread threads[] = new Thread[NUM_CLIENTS];

		for (int i = 0; i < NUM_CLIENTS; i ++){
			try {
				clients[i] = new KVStore("localhost", 50000, NUM_CLIENTS, i);
				threads[i] = new Thread(clients[i]);
				threads[i].start();
			}
			catch (Exception e) {
                ex1 = e;
            }
		}

		for (int i = 0; i < NUM_CLIENTS; i ++){
			try {
                threads[i].join();
			} 
			catch (Exception e) {
                ex2 = e;
            }
		}
		
		assertNull(ex1);
		assertNull(ex2);
	}

	@Test
	public void testdiskStorageGetRequestsStressTest() {
		final int requestNum = 100;
		Random random = ThreadLocalRandom.current();
		byte[] r = new byte[20]; //Means 2048 bit
		random.nextBytes(r);
		String s = new String(r);

		DiskStorage DB = new DiskStorage("ip:port");
		DB.put("k",s);
		for (int i = 0; i < requestNum; i ++){
			assertEquals(s, DB.get("k"));
		}
		DB.clearDisk();
	}

	@Test
	public void testEmptyKey() {
		
		String key = "";
		String value = "testEmptyKey";
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Allow empty string as key
		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
	}
	
	@Test
	public void testMaxLengthKey() {
		String key = "I_AM_MAXIMUM_LENGTH_";	// Maximum length of 20 characters / 20 bytes
		String value = "testMaxLengthKey";
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Maximum allowed length is 20 bytes / 20 characters
		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
	}

	@Test
	public void testKeyExceedingMaxLength() {
		String key = "I_AM_LONGER_THAN_MAXIMUM_LENGTH_";	// Exceeding maximum length of 20 characters / 20 bytes
		String value = "testKeyExceedingMaxLength";
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Exceeding maximum allowed length of 20 bytes / 20 characters
		// an exception should be thrown and no KVMessage returned
		assertTrue(ex != null);
		assertTrue(response == null);
	}

	@Test
	public void testEmptyValue() {
		String key = "testEmptyValue";
		String value = "";
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Empty string as value is a delete operation
		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.GET_ERROR);	
	}

	/**
	 * Helper function to generate random character strings at given length. 
	 * 
	 * @param lenth Length of string to be generated. 
	 * @return Returns the generated string.
	 */
	private String getRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder str = new StringBuilder();
        Random rnd = new Random();
        while (str.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * chars.length());
            str.append(chars.charAt(index));
        }
        return str.toString();
	}
	
	/**
	 * Helper function to generate random byte array at given length and then convert to string before return. 
	 * 
	 * @param lenth Length of array to be generated. 
	 * @return Returns the generated byte array in string format.
	 */
	public String getRandomByteArray(int length) {
		Random rnd = new Random();
		byte[] arr = new byte[length];
		rnd.nextBytes(arr);
		return new String(arr, StandardCharsets.UTF_8);
	}

	@Test
	public void testValueMaxLength() {
		String key = "testValueMaxLength";
		String value = getRandomString(120*1024); // 120 * 1024 bytes / characters
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Maximum allowed length is 120 * 1024 bytes / characters
		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);	
	}

	@Test
	public void testValueExceedingMaxLength() {
		String key = "testValueExceeding";
		String value = getRandomString(120*1024+1); // 120 * 1024 bytes / characters
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Maximum allowed length is 120 * 1024 bytes / characters
		// an exception should be thrown and no KVMessage returned
		assertTrue(ex != null);
		assertTrue(response == null);
	}

	@Test
	public void testSendRamdomByteArray() {
		String key = "testRndByteArr";
		String value = getRandomByteArray(10); // 10 bytes / characters
		KVMessage response = null;
		Exception ex = null;
		try {
			kvClient.connect();
			kvClient.put(key, value);
			kvClient.get(key);
			response = kvClient.recvMessage;
			kvClient.disconnect();
		} 
		catch (Exception e) {
			ex = e;
		}
		// Maximum allowed length is 120 * 1024 bytes / characters
		assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);	
	}

}
