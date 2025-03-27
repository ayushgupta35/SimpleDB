package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private final TransactionId transactionId;
    private OpIterator child;
    private final int tableId;
    private boolean hasInserted;
    private final TupleDesc tupleDesc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.hasInserted = false;
        
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Inserted"});
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasInserted = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasInserted) {
            return null;
        }
        
        int count = 0;
        while (child.hasNext()) {
            Tuple nextTuple = child.next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, nextTuple);
                count++;
            } catch (IOException e) {
                throw new DbException("Error inserting tuple.");
            }
        }
        
        hasInserted = true;
        Tuple result = new Tuple(tupleDesc);
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
