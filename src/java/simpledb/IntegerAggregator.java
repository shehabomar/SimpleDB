package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;
    private Map<Field, Integer> resByGroup;
    private Map<Field, Integer> cntsByGroup;
    private Integer singleRes;
    private Integer singleCnt;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.resByGroup = new HashMap<>();
        this.cntsByGroup = new HashMap<>();
        this.singleCnt = 0;
        this.singleRes = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField aggregateField = (IntField) tup.getField(afield);
        int valueToAggregate = aggregateField.getValue();

        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);

        if (gbfield == NO_GROUPING) {
            updateAggregate(valueToAggregate, null);
        } else {
            updateAggregate(valueToAggregate, groupKey);
        }
    }

    private void updateAggregate(int value, Field groupKey) {
        if (groupKey == null) {
            if (singleRes == null) {
                initializeGroup(value, null);
            } else {
                applyOperation(value, null);
            }
        } else {
            if (!resByGroup.containsKey(groupKey)) {
                initializeGroup(value, groupKey);
            } else {
                applyOperation(value, groupKey);
            }
        }
    }

    private void initializeGroup(int value, Field groupKey) {
        if (groupKey == null) {
            singleCnt = 1;
            // For COUNT, initialize to 1; for others, use the actual value
            singleRes = (what == Op.COUNT) ? 1 : value;
        } else {
            // For COUNT, initialize to 1; for others, use the actual value
            resByGroup.put(groupKey, (what == Op.COUNT) ? 1 : value);
            cntsByGroup.put(groupKey, 1);
        }
    }

    private void applyOperation(int newValue, Field groupKey) {
        int currValue;
        int currCnt;

        if (groupKey == null) {
            currValue = singleRes;
            currCnt = singleCnt;
        } else {
            currCnt = cntsByGroup.get(groupKey);
            currValue = resByGroup.get(groupKey);
        }

        int updatedValue = currValue;
        int updatedCnt = currCnt;

        switch (what) {
            case MIN:
                updatedValue = Math.min(currValue, newValue);
                updatedCnt = currCnt + 1;
                break;
            case MAX:
                updatedValue = Math.max(currValue, newValue);
                updatedCnt = currCnt + 1;
                break;
            case SUM:
            case AVG:
                updatedValue = currValue + newValue;
                updatedCnt = currCnt + 1;
                break;
            case COUNT:
                updatedValue = currValue + 1;
                updatedCnt = currCnt + 1;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + what);
        }

        if (groupKey == null) {
            singleRes = updatedValue;
            singleCnt = updatedCnt;
        } else {
            resByGroup.put(groupKey, updatedValue);
            cntsByGroup.put(groupKey, updatedCnt);
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
        // create tupledesc based on whether we have grouping
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            td = new TupleDesc(new Type[] { gbfieldType, Type.INT_TYPE });
        }

        List<Tuple> tuples = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            // no grouping: return single tuple with aggregate result
            Tuple tuple = new Tuple(td);
            int finalValue = singleRes;

            if (what == Op.AVG) {
                finalValue = singleRes / singleCnt;
            }

            tuple.setField(0, new IntField(finalValue));
            tuples.add(tuple);
        } else {
            // with grouping: return tuple for each group
            for (Map.Entry<Field, Integer> entry : resByGroup.entrySet()) {
                Tuple tuple = new Tuple(td);
                Field groupValue = entry.getKey();
                int aggregateVal = entry.getValue();

                if (what == Op.AVG) {
                    int cnt = cntsByGroup.get(groupValue);
                    aggregateVal = aggregateVal / cnt;
                }

                tuple.setField(0, groupValue);
                tuple.setField(1, new IntField(aggregateVal));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
