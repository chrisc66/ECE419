package DiskStorage;

public interface DiskStorageInterface {

    boolean put(String key, String value) throws Exception;
    String get(String key) throws Exception;
    void clearDisk();
    boolean delelteKV(String key);
    boolean onDisk(String key);
}
