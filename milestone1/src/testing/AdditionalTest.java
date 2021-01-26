package testing;

import org.junit.Test;
import DiskStorage.DiskStorage;
import client.KVStore;

import junit.framework.TestCase;


public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		String keyTest = "dummy";
		String keyTest_1 = "Dummy_11";
		String valTest = "dddddddddddd";

		String valTest_1 = "mmmmmmm";

		DiskStorage DB = new DiskStorage();

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
	public void diskStorageTestBasic(){
		String keyTest = "dummy";
		String valTest = "dddddddddddd";

		DiskStorage DB = new DiskStorage();
		try {
			DB.put(keyTest, valTest);
		}catch (Exception e){

		}
		String Val = DB.get(keyTest);
		assertTrue(valTest==null);
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

}
