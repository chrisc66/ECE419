package DiskStorage;

public class DiskStorageRandomAccess implements DiskStorageInterface{
    @Override
    public boolean put(String key, String value) throws Exception {
        return false;
    }

    @Override
    public String get(String key) throws Exception {
        return null;
    }

    @Override
    public void clearDisk() {

    }

    @Override
    public boolean delelteKV(String key) {
        return false;
    }

    @Override
    public boolean onDisk(String key) {
        return false;
    }
}
