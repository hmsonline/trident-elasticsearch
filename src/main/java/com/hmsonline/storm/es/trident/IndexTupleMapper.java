package com.hmsonline.storm.es.trident;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.Map;

public interface IndexTupleMapper extends Serializable {

    String toIndexName(TridentTuple tuple);

    String toTypeName(TridentTuple tuple);

    String toId(TridentTuple tuple);

    Map<String, Object> toDocument(TridentTuple tuple);

    String toParentId(TridentTuple tuple);

    boolean delete(TridentTuple tuple);

}
