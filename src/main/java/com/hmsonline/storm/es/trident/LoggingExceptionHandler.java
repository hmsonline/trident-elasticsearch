package com.hmsonline.storm.es.trident;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingExceptionHandler implements ExceptionHandler {
    public static final Logger LOG = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public void onElasticSearchException(ElasticSearchException e) {
        LOG.warn("Unexpected exception during index operation", e);
    }

    @Override
    public void onBulkRequestFailure(BulkResponse response) {
        LOG.warn("Bulk request failed: {}", response.buildFailureMessage());
    }
}
