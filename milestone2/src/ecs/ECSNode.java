package ecs;

public class ECSNode implements IECSNode{

    private String nodeName;
    private String nodeHost;
    private String preNodeID;
    private String curNodeID;
    private String nextNodeID;
    private int nodePort;
    private String[] hashRange;

    public STATUS status;

    public ECSNode(String nodeName,String nodeHost, int nodePort, String curNodeIDStart, String[] hashRange){

        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.hashRange = hashRange;
        this.curNodeID = curNodeIDStart;

        preNodeID = null;
        nextNodeID = null;
    }

    public ECSNode(String nodeName,String nodeHost, int nodePort, String curNodeIDStart){

        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.curNodeID = curNodeIDStart;
        this.hashRange = null;

        this.preNodeID = null;
        this.nextNodeID = null;
    }

    public ECSNode(String nodeName,String nodeHost, int nodePort,String curNodeIDStart,
                   String[] hashRange, String preNodeID, String nextNodeID){

        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.hashRange = hashRange;
        this.curNodeID = curNodeIDStart;

        this.preNodeID = preNodeID;
        this.nextNodeID = nextNodeID;
    }


    public void setStatustoOFFLINE(){
        status = STATUS.OFFLINE;
    }

    public void setStatustoINUSE(){
        status = STATUS.INUSE;
    }

    public void setStatustoIDLE(){
        status = STATUS.IDLE;
    }

    public void setStatustoSTOP(){
        status = STATUS.STOP;
    }

    @Override
    public void updateNodeDataBehind(String endRange){
        hashRange[1]= endRange;
        nextNodeID = endRange;
    }

    @Override
    public void updateNodeDataBefore(String prevRange){
        preNodeID = prevRange;
    }


    @Override
    public void setPreNodeID(String preNodeID) {
        this.preNodeID = preNodeID;
    }

    @Override
    public void setNextNodeID(String nextNodeID) {
        this.nextNodeID = nextNodeID;
    }

    @Override
    public String getCurNodeIDStart() {
        return curNodeID;
    }

    public String getNextNodeID() {
        return nextNodeID;
    }

    public String getPreNodeID() {
        return preNodeID;
    }


    @Override
    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getNodeHost() {
        return nodeHost;
    }

    @Override
    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        return hashRange;
    }

    public void setNodeHashRange(String[] hashRange){
        hashRange = hashRange;
    }
}
