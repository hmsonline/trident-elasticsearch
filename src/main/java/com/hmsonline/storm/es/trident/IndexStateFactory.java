package com.hmsonline.storm.es.trident;

import backtype.storm.task.IMetricsContext;
import org.elasticsearch.client.Client;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;


public class IndexStateFactory implements StateFactory {
    public static final int DEFAULT_BATCH_SIZE = 100;

    private ClientFactory clientFactory;
    private ExceptionHandler exceptionHandler;
    private final int batchSize;

    /**
     * IndexFactory for IndexState using default BatchSize (@see #DEFAULT_BATCH_SIZE)
     * @param clientFactory     the Client Factory
     * @param exceptionHandler  the Exception Handler
     */
    public IndexStateFactory(ClientFactory clientFactory, ExceptionHandler exceptionHandler){
        this.clientFactory = clientFactory;
        this.exceptionHandler = exceptionHandler;
        this.batchSize = DEFAULT_BATCH_SIZE;
    }

    /**
     * IndexFactory for IndexState overriding default BatchSize (@see #DEFAULT_BATCH_SIZE), by given value
     * @param clientFactory     the Client Factory
     * @param exceptionHandler  the Exception Handler
     * @param batchSize         the batch size
     */
    public IndexStateFactory(final ClientFactory clientFactory, final ExceptionHandler exceptionHandler, final int batchSize){
        this.clientFactory = clientFactory;
        this.exceptionHandler = exceptionHandler;
        this.batchSize = batchSize;
    }

    @Override
    public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i2) {
        Client client = this.clientFactory.makeClient(conf);
        return new IndexState(client, this.exceptionHandler, this.batchSize);
    }
}
