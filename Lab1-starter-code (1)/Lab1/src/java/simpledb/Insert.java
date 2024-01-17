package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private TupleDesc child_td;
    private TupleDesc td;
    private int tableId;
    private TransactionId tid;
    private int num_ins_rec; 
    private boolean retrieved;

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
        // some code goes here
        this.tableId = tableId;
        this.tid = t;
        this.child = child;
        
        this.child_td = child.getTupleDesc();
        
        TupleDesc table_td = Database.getCatalog().getTupleDesc(tableId);
        
        if (!table_td.equals(child_td)) {
            throw new DbException("Mismatched TupleDesc: Child does not match the table"); // verify if it matchs
        }
    
        Type[] typeArray = new Type[] {Type.INT_TYPE};
        td = new TupleDesc(typeArray); 
        this.retrieved = false;
        }        
    

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        //some code goes here
        child.open();
        super.open();
    
        while (child.hasNext()) {
            Tuple nextTuple = child.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, nextTuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
    
            this.num_ins_rec++;
        }
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
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
        // some code goes here
    	Tuple num_rec_t = null;
    	if (!retrieved) {
	    	num_rec_t = new Tuple(this.td);	    	
	    	num_rec_t.setField(0, new IntField(this.num_ins_rec));
	    	retrieved = true;
    	} 
    	
        return num_rec_t;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	if (this.child!=children[0])
    	{
    	    this.child = children[0];
    	}
    }
}
