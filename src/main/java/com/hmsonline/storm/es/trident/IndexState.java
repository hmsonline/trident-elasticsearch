package com.hmsonline.storm.es.trident;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class IndexState implements State {
    public static final Logger LOG = LoggerFactory.getLogger(IndexState.class);
    private Client client;
    private ExceptionHandler exceptionHandler;
    private final int batchSize;

    /**
     * @param client the Client
     * @param exceptionHandler the Exception Handler
     * @param maxBatchSize specifying the batch size for ES bulk operations
     */
    public IndexState(final Client client, final ExceptionHandler exceptionHandler, final int maxBatchSize){
        this.client = client;
        this.exceptionHandler = exceptionHandler;
        this.batchSize = maxBatchSize;
    }

    @Override
    public void beginCommit(Long aLong) {

    }

    @Override
    public void commit(Long aLong) {

    }

    public void updateState(List<TridentTuple> tridentTuples, IndexTupleMapper mapper, TridentCollector tridentCollector){
        int i = 0;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(TridentTuple tuple : tridentTuples){
            if(! mapper.delete(tuple)){
                Map<String, Object> document = mapper.toDocument(tuple);
                String id = mapper.toId(tuple);
                if(document != null && id != null) {
                    String parentId = mapper.toParentId(tuple);
                    if(StringUtils.isEmpty(parentId)){
                        bulkRequest.add(client.prepareIndex(
                                mapper.toIndexName(tuple),
                                mapper.toTypeName(tuple),
                                id
                                ).setSource(document));
                    } else {
                        bulkRequest.add(client.prepareIndex(
                                mapper.toIndexName(tuple),
                                mapper.toTypeName(tuple),
                                id
                        ).setSource(document).setParent(parentId));
                    }
                }
            } else {
                bulkRequest.add(client.prepareDelete(
                        mapper.toIndexName(tuple),
                        mapper.toTypeName(tuple),
                        mapper.toId(tuple)
                ));
            }
            i++;
            if (i >= batchSize) {
                runBulk(bulkRequest);
                bulkRequest = client.prepareBulk();
                i = 0;
            }
        }
        if (i > 0) {
            runBulk(bulkRequest);
        }
    }

    private void runBulk(BulkRequestBuilder bulkRequest) {
        if(bulkRequest.numberOfActions() > 0) {
            int tryCount = 0;
            boolean shouldTryAgain;
            do {
                shouldTryAgain = false;
                try {                
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if (bulkResponse.hasFailures()) {
                        shouldTryAgain = this.exceptionHandler.onBulkRequestFailure(bulkResponse, tryCount);
                        tryCount++;
                    }
                } catch (ElasticsearchException e) {
                    shouldTryAgain = this.exceptionHandler.onElasticSearchException(e, tryCount);
                    tryCount++;
                }
            } while (shouldTryAgain);
        } else {
            LOG.debug("Empty batch being submitted");
        }
    }
}
