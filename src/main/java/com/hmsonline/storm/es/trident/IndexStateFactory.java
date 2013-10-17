package com.hmsonline.storm.es.trident;

import backtype.storm.task.IMetricsContext;
import org.elasticsearch.client.Client;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;


public class IndexStateFactory implements StateFactory {
    private ClientFactory clientFactory;
    private ExceptionHandler exceptionHandler;

    public IndexStateFactory(ClientFactory clientFactory, ExceptionHandler exceptionHandler){
        this.clientFactory = clientFactory;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {
        Client client = this.clientFactory.makeClient(conf);
        return new IndexState(client, this.exceptionHandler);
    }
}
