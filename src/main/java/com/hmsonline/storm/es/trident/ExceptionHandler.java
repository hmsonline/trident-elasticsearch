package com.hmsonline.storm.es.trident;


import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.Serializable;

public interface ExceptionHandler extends Serializable {

    void onElasticSearchException(ElasticSearchException e);

    void onBulkRequestFailure(BulkResponse response);
}
