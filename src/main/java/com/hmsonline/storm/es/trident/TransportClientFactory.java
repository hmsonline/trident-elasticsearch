package com.hmsonline.storm.es.trident;


import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransportClientFactory implements ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TransportClientFactory.class);
    public static final int DEFAULT_PORT = 9300;


    @Override
    public Client makeClient(Map conf) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

        settingsBuilder.put("cluster.name", (String)conf.get(CLUSTER_NAME));

        TransportClient client = new TransportClient(settingsBuilder);

        String clusterHosts = (String)conf.get(CLUSTER_HOSTS);

        LOG.info("Creating TransportClient with addresses: '{}'", clusterHosts);

        // expecting "host:port,host2:port2,host3"
        if(!StringUtils.isEmpty(clusterHosts)){
            String[] hostPorts = StringUtils.split(clusterHosts, ",");
            for (String hostPortStr : hostPorts){
                String[] hostPort = StringUtils.split(hostPortStr, ":");
                if(hostPort.length == 2){
                    client.addTransportAddress(new InetSocketTransportAddress(hostPort[0], Integer.parseInt(hostPort[1])));
                } else if (hostPort.length == 1){
                    client.addTransportAddress(new InetSocketTransportAddress(hostPort[0], DEFAULT_PORT));
                }

            }

        } else {
            throw new IllegalStateException("Settings for '" + CLUSTER_HOSTS + "' can not be empty when using the Transport Client");
        }

        return client;
    }
}
