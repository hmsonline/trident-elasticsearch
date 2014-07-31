package com.hmsonline.storm.es.trident;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryWithSleepExceptionHandler implements ExceptionHandler {
    private static final long serialVersionUID = 5104052711474301016L;
    public static final Logger LOG = LoggerFactory.getLogger(RetryWithSleepExceptionHandler.class);
    
    private int retries;
    private int sleepTime;
    
    public RetryWithSleepExceptionHandler(int retries, int sleepTime) {
        this.retries = retries;
        this.sleepTime = sleepTime;
    }

    @Override
    public boolean onElasticSearchException(ElasticsearchException e, int tryNumber) {
        if (tryNumber < retries) {
            LOG.warn("Retry: Unexpected exception during index operation", e.getDetailedMessage());
            try {
                Thread.sleep(sleepTime * (tryNumber + 1));
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
            return true;
        } else {
            LOG.warn("Unexpected exception during index operation", e);
            return false;    
        }
    }

    @Override
    public boolean onBulkRequestFailure(BulkResponse response, int tryNumber) {
        if (tryNumber < retries) {
            LOG.warn("Retry: Bulk request failed: {}", response.buildFailureMessage());
            try {
                Thread.sleep(sleepTime * (tryNumber + 1));
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
            return true;
        } else {
            LOG.warn("Bulk request failed: {}", response.buildFailureMessage());
            return false;
        }
    }
}
