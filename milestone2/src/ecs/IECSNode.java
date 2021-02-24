package ecs;

public interface IECSNode {
    void setNextNodeID(String toString);

    void setPreNodeID(String toString);

    void setNodeHashRange(String[] hashRange);

    String getCurNodeIDStart();

    void updateNodeDataBehind(String toString);

    void updateNodeDataBefore(String toString);

    enum STATUS{
        OFFLINE,    // nodes sitting in ecs config not added
        IDLE,       // nodes added but not started
        INUSE,      // nodes started
        STOP        // nodes shutdown 
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

}
