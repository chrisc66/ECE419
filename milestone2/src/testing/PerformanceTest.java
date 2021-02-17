package testing;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import client.KVStore;

import junit.framework.TestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class PerformanceTest extends TestCase {

    private KVStore kvClient;
    private byte[] data_16, data_512, data_2048, data_4096;
    private String data_16_str, data_512_str, data_2048_str, data_4096_str;
    private String key = "foo";
    Random r = new Random(10);

    @BeforeClass
    public void setUp() {
        kvClient = new KVStore("localhost", 50000);

        data_16 = new byte[16];
        data_512 = new byte[512];
        data_2048 = new byte[2048];
        data_4096 = new byte[4096];

        r.nextBytes(data_16);
        r.nextBytes(data_512);
        r.nextBytes(data_2048);
        r.nextBytes(data_4096);

        data_16_str = new String(data_16, StandardCharsets.UTF_8);
        data_512_str = new String(data_512, StandardCharsets.UTF_8);
        data_2048_str = new String(data_2048, StandardCharsets.UTF_8);
        data_4096_str = new String(data_4096, StandardCharsets.UTF_8);

        try {
            kvClient.connect();
        } catch (Exception e) {
            System.err.println("Client Connection Failed!");
        }
    }

    @AfterClass
    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPerf_8020() {
        int iteration = 100000;
        int numBytes = 16;
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (4 * numBytes + getOutput.getBytes(StandardCharsets.UTF_8).length);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5);

        System.out.println("Perf (80% put & 20% get)    = " + perf + " KB/s");
        System.out.println("Latency (80% put & 20% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_5050() {
        int iteration = 250000;
        int numBytes = 16;
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes + getOutput.getBytes(StandardCharsets.UTF_8).length);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 2);

        System.out.println("Perf (50% put & 50% get)    = " + perf + " KB/s");
        System.out.println("Latency (50% put & 50% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_2080() {
        int iteration = 100000;
        int numBytes = 16;
        String getOutput = "";
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);

                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes + 4 * getOutput.getBytes(StandardCharsets.UTF_8).length);

        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5.0);

        System.out.println("Perf (20% put & 80% get)    = " + perf + " KB/s");
        System.out.println("Latency (20% put & 80% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_1000() {
        int iteration = 100000;
        int numBytes = 16;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
                kvClient.put(key, data_16_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * numBytes *5;
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5);

        System.out.println("Perf (100% put & 0% get)    = " + perf + " KB/s");
        System.out.println("Latency (100% put & 0% get) = " + latency + " ms");
    }

    @Test
    public void testPerf_0100() {
        int iteration = 100000;
        int numBytes = 16;
        String getOutput = "";
        try {
            kvClient.put(key, data_16_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
                getOutput = kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * getOutput.getBytes(StandardCharsets.UTF_8).length * 5;
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 5);

        System.out.println("Perf (0% put & 100% get)    = " + perf + " KB/s");
        System.out.println("Latency (0% put & 100% get) = " + latency + " ms");
    }


//------------------------------------compare between different payload (same iteration)----------------------------------------//
@Test
public void testPerf_put_16_() {
    int iteration = 20000;
    int numBytes = 16;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iteration; i++) {
        try {
            kvClient.put(key, data_16_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
    }

    long duration = System.currentTimeMillis() - startTime;

    double totalBytes = iteration * (numBytes);
    double perf = 1000.0 * totalBytes / (1024 * duration);
    double latency = 1000 * duration / (iteration * 1);

    System.out.println("Perf (put payload 16)    = " + perf + " KB/s");
    System.out.println("Latency (put payload 16) = " + latency + " ms");
}

    @Test
    public void testPerf_get_16_() {
        int iteration = 20000;
        int numBytes = 16;
        try {
            kvClient.put(key, data_16_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (get payload 16)    = " + perf + " KB/s");
        System.out.println("Latency (get payload 16) = " + latency + " ms");
    }

    @Test
    public void testPerf_put_512_() {
        int iteration = 20000;
        int numBytes = 512;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_512_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (put payload 512)    = " + perf + " KB/s");
        System.out.println("Latency (put payload 512) = " + latency + " ms");
    }

    @Test
    public void testPerf_get_512_() {
        int iteration = 20000;
        int numBytes = 512;
        try {
            kvClient.put(key, data_512_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (get payload 512)    = " + perf + " KB/s");
        System.out.println("Latency (get payload 512) = " + latency + " ms");
    }

    @Test
    public void testPerf_put_2048_() {
        int iteration = 20000;
        int numBytes = 2048;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_2048_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (put payload 2048)    = " + perf + " KB/s");
        System.out.println("Latency (put payload 2048) = " + latency + " ms");
    }

    @Test
    public void testPerf_get_2048_() {
        int iteration = 20000;
        int numBytes = 2048;
        try {
            kvClient.put(key, data_2048_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (get payload 2048)    = " + perf + " KB/s");
        System.out.println("Latency (get payload 2048) = " + latency + " ms");
    }

    @Test
    public void testPerf_put_4096_() {
        int iteration = 20000;
        int numBytes = 4096;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.put(key, data_4096_str);
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (put payload 4096)    = " + perf + " KB/s");
        System.out.println("Latency (put payload 4096) = " + latency + " ms");
    }

    @Test
    public void testPerf_get_4096_() {
        int iteration = 20000;
        int numBytes = 4096;
        try {
            kvClient.put(key, data_4096_str);
        } catch (Exception e) {
            System.out.println("Performance test error!");
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            try {
                kvClient.get(key).getValue();
            } catch (Exception e) {
                System.out.println("Performance test error!");
            }
        }

        long duration = System.currentTimeMillis() - startTime;

        double totalBytes = iteration * (numBytes);
        double perf = 1000.0 * totalBytes / (1024 * duration);
        double latency = 1000 * duration / (iteration * 1);

        System.out.println("Perf (get payload 4096)    = " + perf + " KB/s");
        System.out.println("Latency (get payload 4096) = " + latency + " ms");
    }

}

