package com.hmsonline.storm.es.trident;

import storm.trident.tuple.TridentTuple;

import java.util.Map;

public abstract class BaseIndexTupleMapper implements IndexTupleMapper {

    @Override
    public boolean delete(TridentTuple tuple) {
        return false;
    }

    @Override
    public String toParentId(TridentTuple tuple) {
        return null;
    }

    @Override
    public String toId(TridentTuple tuple) {
        return null;
    }
}
