package com.hmsonline.storm.es.trident;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class IndexState implements State {
    private static final int MAX_BATCH_SIZE = 100;
    private Client client;
    private ExceptionHandler exceptionHandler;

    public IndexState(Client client, ExceptionHandler exceptionHandler){
        this.client = client;
        this.exceptionHandler = exceptionHandler;
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
            if(i >= MAX_BATCH_SIZE) {
                try{
                    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                    if(bulkResponse.hasFailures()){
                        this.exceptionHandler.onBulkRequestFailure(bulkResponse);
                    }
                } catch(ElasticsearchException e){
                    this.exceptionHandler.onElasticSearchException(e);
                } 
                bulkRequest = client.prepareBulk();
                i = 0;
            }
        }
        if (i > 0) {
            try {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    this.exceptionHandler.onBulkRequestFailure(bulkResponse);
                }
            } catch (ElasticsearchException e) {
                this.exceptionHandler.onElasticSearchException(e);
            }
        }
    }
}
