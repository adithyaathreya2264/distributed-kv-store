package com.dkv.network;

public class ClusterNodeAddress {
    private final String nodeId;
    private final String host;
    private final int port;

    public ClusterNodeAddress(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "ClusterNodeAddress{nodeId='" + nodeId + "', host='" + host + "', port=" + port + "}";
    }
}
