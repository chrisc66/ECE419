package shared.messages;

import java.math.BigInteger;

public class Metadata {
    public String serverAddress;
    public Integer port;
    public BigInteger start;    // start of hash range (value of current ECSNode / KVServer)
    public BigInteger stop;     // end of hash range
    public Metadata(String serverAddress_, Integer port_, BigInteger start_, BigInteger stop_){
        serverAddress = serverAddress_;
        port = port_;
        start = start_;
        stop = stop_;
    }
}
