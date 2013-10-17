package com.hmsonline.storm.es.trident;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class IndexState implements State {

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
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(TridentTuple tuple : tridentTuples){
            if(! mapper.delete(tuple)){
                String parentId = mapper.toParentId(tuple);
                if(StringUtils.isEmpty(parentId)){
                    bulkRequest.add(client.prepareIndex(
                            mapper.toIndexName(tuple),
                            mapper.toTypeName(tuple),
                            mapper.toId(tuple)
                            ).setSource(mapper.toDocument(tuple)));
                } else {
                    bulkRequest.add(client.prepareIndex(
                            mapper.toIndexName(tuple),
                            mapper.toTypeName(tuple),
                            mapper.toId(tuple)
                    ).setSource(mapper.toDocument(tuple)).setParent(parentId));
                }
            } else {
                bulkRequest.add(client.prepareDelete(
                        mapper.toIndexName(tuple),
                        mapper.toTypeName(tuple),
                        mapper.toId(tuple)
                ));
            }
        }
        try{
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if(bulkResponse.hasFailures()){
                this.exceptionHandler.onBulkRequestFailure(bulkResponse);
            }
        } catch(ElasticSearchException e){
            this.exceptionHandler.onElasticSearchException(e);
        }
    }
}
