package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupFieldIndex;
    private Type groupFieldType;
    private HashMap<Field, Integer> countMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT operation.");
        }
        this.groupFieldIndex = gbfield;
        this.groupFieldType = gbfieldtype;
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = (groupFieldIndex == Aggregator.NO_GROUPING) ? null : tup.getField(groupFieldIndex);
        countMap.put(groupKey, countMap.getOrDefault(groupKey, 0) + 1);
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
        ArrayList<Tuple> resultTuples = new ArrayList<>();
        TupleDesc desc;

        if (groupFieldIndex == Aggregator.NO_GROUPING) {
            desc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        } else {
            desc = new TupleDesc(
                new Type[]{groupFieldType, Type.INT_TYPE},
                new String[]{"groupValue", "aggregateValue"}
            );
        }

        for (Field key : countMap.keySet()) {
            int count = countMap.get(key);
            Tuple tuple = new Tuple(desc);

            if (groupFieldIndex == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(count));
            } else {
                tuple.setField(0, key);
                tuple.setField(1, new IntField(count));
            }
            resultTuples.add(tuple);
        }
        return new TupleIterator(desc, resultTuples);
    }

}
