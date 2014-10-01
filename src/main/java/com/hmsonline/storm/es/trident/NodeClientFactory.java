package com.hmsonline.storm.es.trident;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeClientFactory implements ClientFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NodeClientFactory.class);
    private static Map<String, Node> NODES = new HashMap<String, Node>();
    private static Object MUTEX = new Object();

    @Override
    @SuppressWarnings("rawtypes")
    public Client makeClient(Map conf) {
        String clusterName = (String) conf.get(CLUSTER_NAME);
        synchronized (MUTEX) {
            LOG.info("Attaching node client to cluster: '{}'", clusterName);
            Node node = NODES.get(clusterName);
            if (node == null) {
                node = nodeBuilder().clusterName(clusterName).client(true).data(false).node();
                NODES.put(clusterName, node);
            }
            return node.client();
        }
    }
}
