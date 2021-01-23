package DiskStorage;


public class KeyValuePair {
    private String key;
    private String value;
    private long startPtr;
    private long endPtr;

    public KeyValuePair(long startPtr, long endPtr){
        this.startPtr = startPtr;
        this.endPtr = endPtr;
    }
    public KeyValuePair(String key, String val){
        this.key= key;
        this.value = val;
    }
    public KeyValuePair(long startPtr, long endPtr,String key, String val){
        this.endPtr= endPtr;
        this.startPtr = startPtr;
        this.key= key;
        this.value =val;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getEndPtr() {
        return endPtr;
    }

    public long getStartPtr() {
        return startPtr;
    }

    public void setEndPtr(long endPtr) {
        this.endPtr = endPtr;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setStartPtr(long startPtr) {
        this.startPtr = startPtr;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
