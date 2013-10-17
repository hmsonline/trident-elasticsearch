package com.hmsonline.storm.es.trident;

import org.elasticsearch.client.Client;

import java.io.Serializable;
import java.util.Map;

public interface ClientFactory extends Serializable {
    public static final String CLUSTER_NAME = "com.hmsonline.storm.es.clustername";
    public static final String CLUSTER_HOSTS = "com.hmsonline.storm.es.hosts";

    Client makeClient(Map conf);
}
