package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private final int gbfield;
    private final int afield;
    private final Type gbFieldType;
    private final TupleDesc td;
    private Op op;
    private Map<Field, List<Integer>> gbstatistics = new HashMap<>();
    private List<Tuple> resTuples = new LinkedList<>();
    private AggHandler handler;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        switch(op) {
            case COUNT: {
                handler = new StringAggregator.StringCountHandler();
                break;
            }
            default:
                //throw new NoSuchElementException();
        }
        if (gbfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[] {gbFieldType, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            handler.calculate(new IntField(-1), (StringField) tup.getField(afield));
        } else {
            handler.calculate(tup.getField(gbfield), (StringField) tup.getField(afield));
        }
    }

    private class StringCountHandler extends AggHandler {
        @Override
        void calculate(Field groupBy, Field aggregate) {
            if (!res.containsKey(groupBy)) {
                res.put(groupBy, 1);
            } else {
                res.put(groupBy, res.get(groupBy) + 1);
            }
        }
    }
    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        for (Map.Entry<Field, Integer> entry: handler.res.entrySet()) {
            Tuple tp = new Tuple(td);
            int value= (int) entry.getValue();
            if (gbfield == NO_GROUPING) {
                tp.setField(0,new IntField(value));
            } else {
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(value));
            }
            resTuples.add(tp);
        }
        return new TupleIterator(td, resTuples);
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }
}
