package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupCnts;
    private int overallCnt;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;

        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports count");
        }

        this.what = what;
        this.groupCnts = new ConcurrentHashMap<>();
        this.overallCnt = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            overallCnt++;
        } else {
            Field groupVal = tup.getField(gbfield);
            if (groupCnts.containsKey(groupVal)) {
                groupCnts.put(groupVal, groupCnts.get(groupVal) + 1);
            } else {
                groupCnts.put(groupVal, 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc shcema;
        if (gbfield == NO_GROUPING) {
            // single col
            shcema = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            // two cols
            shcema = new TupleDesc(new Type[] { gbfieldType, Type.INT_TYPE });
        }

        List<Tuple> resTuples = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            Tuple resTuple = new Tuple(shcema);
            resTuple.setField(0, new IntField(overallCnt));
            resTuples.add(resTuple);
        } else {
            for (Map.Entry<Field, Integer> entry : groupCnts.entrySet()) {
                Tuple resTuple = new Tuple(shcema);
                resTuple.setField(0, entry.getKey());
                resTuple.setField(1, new IntField(entry.getValue()));
                resTuples.add(resTuple);
            }
        }
        return new TupleIterator(shcema, resTuples);
    }

}
