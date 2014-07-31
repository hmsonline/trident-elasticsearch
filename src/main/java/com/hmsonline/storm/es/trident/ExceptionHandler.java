package com.hmsonline.storm.es.trident;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.Serializable;

public interface ExceptionHandler extends Serializable {
    
    /**
     *  
     * @param e
     * @param tryNumber First try is 0.
     * @return True to retry and false to continue.
     */
    boolean onElasticSearchException(ElasticsearchException e, int tryNumber);

    /**
     *  
     * @param response
     * @param tryNumber First try is 0.
     * @return True to retry and false to continue.
     */
    boolean onBulkRequestFailure(BulkResponse response, int tryNumber);
}
