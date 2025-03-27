package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupFieldIndex;
    private Type groupFieldType;
    private int aggregateFieldIndex;
    private Op aggregationOp;
    private HashMap<Field, Integer> aggregateValues;
    private HashMap<Field, Integer> groupCounts;

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
        this.groupFieldIndex = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.aggregationOp = what;
        this.aggregateValues = new HashMap<>();
        this.groupCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = (groupFieldIndex == Aggregator.NO_GROUPING) ? null : tup.getField(groupFieldIndex);

        if (!aggregateValues.containsKey(groupField)) {

            int initializeAggregateValue;
        
            switch (aggregationOp) {
                case MIN:
                    initializeAggregateValue = Integer.MAX_VALUE;
                    break;
                case MAX:
                    initializeAggregateValue = Integer.MIN_VALUE;
                    break;
                case SUM:
                case AVG:
                case COUNT:
                    initializeAggregateValue = 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported aggregation operation");
            }
            aggregateValues.put(groupField, initializeAggregateValue);
            groupCounts.put(groupField, 0);
        }        

        int tupleValue = ((IntField) tup.getField(aggregateFieldIndex)).getValue();
        int currentValue = aggregateValues.get(groupField);
        int count = groupCounts.get(groupField);

        switch (aggregationOp) {
            case MIN:
                aggregateValues.put(groupField, Math.min(currentValue, tupleValue));
                break;
            case MAX:
                aggregateValues.put(groupField, Math.max(currentValue, tupleValue));
                break;
            case SUM:
            case AVG:
                groupCounts.put(groupField, count + 1);
                aggregateValues.put(groupField, currentValue + tupleValue);
                break;
            case COUNT:
                aggregateValues.put(groupField, currentValue + 1);
                break;
            default:
                throw new IllegalArgumentException("Unsupported aggregation operation");
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
        ArrayList<Tuple> results = new ArrayList<>();

        TupleDesc desc = null;

        if (groupFieldIndex == Aggregator.NO_GROUPING) {
            desc = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateValue" });
        } else {
            desc = new TupleDesc(
                new Type[] { groupFieldType, Type.INT_TYPE },
                new String[] { "groupValue", "aggregateValue" }
            );
        }

        for (Field groupKey : aggregateValues.keySet()) {
            int aggregateResult = aggregationOp == Op.AVG 
                ? aggregateValues.get(groupKey) / groupCounts.get(groupKey) 
                : aggregateValues.get(groupKey);

            Tuple resultTuple = new Tuple(desc);
            if (groupFieldIndex == Aggregator.NO_GROUPING) {
                resultTuple.setField(0, new IntField(aggregateResult));
            } else {
                resultTuple.setField(0, groupKey);
                resultTuple.setField(1, new IntField(aggregateResult));
            }
            results.add(resultTuple);
        }

        return new TupleIterator(desc, results);
    }

}
