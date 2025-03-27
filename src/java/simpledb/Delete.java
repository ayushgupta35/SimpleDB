package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private OpIterator child;
    private boolean hasDeleted;
    private final TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.child = child;
        this.hasDeleted = false;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Deleted"});
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasDeleted = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasDeleted) {
            return null;
        }
        
        int count = 0;
        while (child.hasNext()) {
            Tuple nextTuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, nextTuple);
                count++;
            } catch (IOException e) {
                throw new DbException("Error deleting tuple.");
            }
        }
        
        hasDeleted = true;
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
