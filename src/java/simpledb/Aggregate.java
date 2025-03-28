package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator sourceIterator;
    private int aggFieldIndex;
    private int groupFieldIndex;
    private Aggregator.Op aggregationOp;
    private Aggregator aggregator;
    private OpIterator resultIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.sourceIterator = child;
        this.aggFieldIndex = afield;
        this.groupFieldIndex = gfield;
        this.aggregationOp = aop;
        this.resultIterator = null;

        Type groupType = (groupFieldIndex == Aggregator.NO_GROUPING) ? null
                : sourceIterator.getTupleDesc().getFieldType(groupFieldIndex);

        Type aggType = sourceIterator.getTupleDesc().getFieldType(aggFieldIndex);

        if (aggType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(groupFieldIndex, groupType, aggFieldIndex, aggregationOp);
        } else if (aggType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(groupFieldIndex, groupType, aggFieldIndex, aggregationOp);
        } else {
            throw new IllegalArgumentException("Unsupported aggregate type.");
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return groupFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        return (groupFieldIndex == Aggregator.NO_GROUPING) ? null
                : sourceIterator.getTupleDesc().getFieldName(groupFieldIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return sourceIterator.getTupleDesc().getFieldName(aggFieldIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return aggregationOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        sourceIterator.open();
        while (sourceIterator.hasNext()) {
            aggregator.mergeTupleIntoGroup(sourceIterator.next());
        }
        resultIterator = aggregator.iterator();
        resultIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        return resultIterator.hasNext() ? resultIterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        resultIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc childTupleDesc = sourceIterator.getTupleDesc();
    
        if (groupFieldIndex == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{childTupleDesc.getFieldType(aggFieldIndex)}, 
                                new String[]{childTupleDesc.getFieldName(aggFieldIndex)});
        } else {
            return new TupleDesc(new Type[]{childTupleDesc.getFieldType(groupFieldIndex), childTupleDesc.getFieldType(aggFieldIndex)}, 
                                new String[]{childTupleDesc.getFieldName(groupFieldIndex), childTupleDesc.getFieldName(aggFieldIndex)});
        }
    }

    public void close() {
        super.close();
        sourceIterator.close();
        resultIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { resultIterator };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length > 0) {
            this.resultIterator = children[0];
        }
    } 
}
