package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.HashMap;
import java.util.Map;

public abstract class AggHandler {
    Map<Field, Integer> res = new HashMap<>();
    abstract void calculate(Field groupBy, Field aggregate);
}
