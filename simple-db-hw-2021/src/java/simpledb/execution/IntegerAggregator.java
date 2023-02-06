package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.logging.Handler;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final int afield;
    private final Type gbFieldType;
    private Op op;
    public final TupleDesc td;
    private AggHandler handler;
    private List<Tuple> resTuples = new LinkedList<>();
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        switch(op) {
            case MIN: {
                handler = new MinHandler();
                break;
            }
            case MAX: {
                handler = new MaxHandler();
                break;
            }
            case SUM: {
                handler = new SumHandler();
                break;
            }
            case AVG: {
                handler = new AvgHandler();
                break;
            }
            case COUNT: {
                handler = new CountHandler();
                break;
            }
        }
        if (gbfield == Aggregator.NO_GROUPING) {
            this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            this.td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            handler.calculate(new IntField(-1), (IntField) tup.getField(afield));
        } else {
            handler.calculate(tup.getField(gbfield), (IntField) tup.getField(afield));
        }

    }

    private class MinHandler extends AggHandler {
        @Override
        void calculate(Field groupBy, Field aggregate) {
            if (!res.containsKey(groupBy)) {
                res.put(groupBy, ((IntField) aggregate).getValue());
            } else {
                res.put(groupBy, Math.min(res.get(groupBy), ((IntField) aggregate).getValue()));
            }
        }
    }

    private class MaxHandler extends AggHandler {
        @Override
        void calculate(Field groupBy, Field aggregate) {
            if (!res.containsKey(groupBy)) {
                res.put(groupBy, ((IntField) aggregate).getValue());
            } else {
                res.put(groupBy, Math.max(res.get(groupBy), ((IntField) aggregate).getValue()));
            }
        }
    }

    private class SumHandler extends AggHandler {
        @Override
        void calculate(Field groupBy, Field aggregate) {
            if (!res.containsKey(groupBy)) {
                res.put(groupBy, ((IntField) aggregate).getValue());
            } else {
                res.put(groupBy, res.get(groupBy) + ((IntField) aggregate).getValue());
            }
        }
    }

    private class AvgHandler extends AggHandler {
        public HashMap<Field, Integer> groupCount = new HashMap<>();
        public HashMap<Field, Integer> groupSum = new HashMap<>();
        @Override
        void calculate(Field groupBy, Field aggregate) {
            if (!res.containsKey(groupBy)) {
                groupSum.put(groupBy, ((IntField) aggregate).getValue());
                groupCount.put(groupBy, 1);
            } else {
                groupSum.put(groupBy, groupSum.get(groupBy) + ((IntField) aggregate).getValue());
                groupCount.put(groupBy, groupCount.get(groupBy) + 1);
            }
            res.put(groupBy, groupSum.get(groupBy) / groupCount.get(groupBy));
        }
    }

    private class CountHandler extends AggHandler {
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
    };

    public TupleDesc getTupleDesc() {
        return this.td;
    }


}
