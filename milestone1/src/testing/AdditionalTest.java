package testing;

import org.junit.Test;
import DiskStorage.DiskStorage;

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
}
