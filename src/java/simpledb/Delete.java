package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private boolean alreadyFetched;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *              The transaction this delete runs in
     * @param child
     *              The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.alreadyFetched = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        alreadyFetched = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        alreadyFetched = false;
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
        if (alreadyFetched) {
            return null;
        }

        alreadyFetched = true;
        int count = 0;

        try {
            while (child.hasNext()) {
                Tuple t = child.next();
                Database.getBufferPool().deleteTuple(tid, t);
                count++;
            }
        } catch (IOException e) {
            throw new DbException("Error deleting tuple: " + e.getMessage());
        }

        Tuple result = new Tuple(getTupleDesc());
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length > 0) {
            child = children[0];
        }
    }

}
