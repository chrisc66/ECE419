package shared.messages;

import java.math.BigInteger;

public class Metadata {
    public String serverAddress;
    public Integer port;
    public BigInteger start;
    public BigInteger stop;
    public Metadata(String serverAddress_, Integer port_, BigInteger start_, BigInteger stop_){
        serverAddress = serverAddress_;
        port = port_;
        start = start_;
        stop = stop_;
    }
}
