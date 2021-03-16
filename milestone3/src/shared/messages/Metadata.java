package shared.messages;

import java.math.BigInteger;

public class Metadata {
    public String serverAddress;
    public Integer port;
    public BigInteger prev;     // start of previous hash range
    public BigInteger start;    // start of current hash range (value of current ECSNode / KVServer)
    public BigInteger stop;     // end of current hash range / start of next hash range
    public Metadata(String serverAddress_, Integer port_, BigInteger prev_, BigInteger start_, BigInteger stop_){
        serverAddress = serverAddress_;
        port = port_;
        prev = prev_;
        start = start_;
        stop = stop_;
    }
}
