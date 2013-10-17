package com.hmsonline.storm.es.trident;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class IndexUpdater extends BaseStateUpdater<IndexState> {

    private IndexTupleMapper mapper;

    public IndexUpdater(IndexTupleMapper mapper){
        this.mapper = mapper;
    }


    @Override
    public void updateState(IndexState indexState, List<TridentTuple> tridentTuples, TridentCollector tridentCollector) {
        indexState.updateState(tridentTuples, this.mapper, tridentCollector);
    }
}
