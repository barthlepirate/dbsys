package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private TupleDesc td;
    private TransactionId tid;
    private int num_del_rec; 
    private boolean retrieved;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    public Delete(TransactionId transactionId, OpIterator child) {
        this.tid = transactionId;
        this.child = child;
        Type[] typeArray = new Type[]{Type.INT_TYPE};
        td = new TupleDesc(typeArray); 
        this.retrieved = false;
    }
    

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    
        while (child.hasNext()) {
            Tuple nextTuple = child.next();
    
            try {
                Database.getBufferPool().deleteTuple(this.tid, nextTuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
    
            this.num_del_rec++;
        }
    }    

    public void close() {
        // some code goes here
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
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
        Tuple resultTuple = null;
        if (!retrieved) {
            resultTuple = new Tuple(this.td);
            resultTuple.setField(0, new IntField(this.num_del_rec));
            retrieved = true;
        }
        return resultTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length > 0 && this.child != children[0]) {
            this.child = children[0];
        }
    }    

}