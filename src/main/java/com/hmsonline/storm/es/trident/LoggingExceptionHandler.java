package com.hmsonline.storm.es.trident;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingExceptionHandler implements ExceptionHandler {
    private static final long serialVersionUID = 5104052711474301016L;
    public static final Logger LOG = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public boolean onElasticSearchException(ElasticsearchException e, int tryNumber) {
        LOG.warn("Unexpected exception during index operation", e);
        return false;
    }

    @Override
    public boolean onBulkRequestFailure(BulkResponse response, int tryNumber) {
        LOG.warn("Bulk request failed: {}", response.buildFailureMessage());
        return false;
    }
}
