package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */

    private String tableAlias;
    private int tableid;
    private TransactionId tid;
    private TupleDesc td;
    private Iterator<Tuple> iterator;

    
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.td = Database.getCatalog().getTupleDesc(tableid);
        this.iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }   

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableAlias = null; this.tableid = 0;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // Vérifiez si l'iterator est déjà ouvert
        if (iterator == null) {
            throw new DbException("Iterator not initialized");
    }
    }    

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc originalTd = Database.getCatalog().getTupleDesc(tableid);
        int numFields = originalTd.numFields();
        Type[] fieldTypes = new Type[numFields];
        String[] fieldNames = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldTypes[i] = originalTd.getFieldType(i);
            fieldNames[i] = tableAlias + "." + originalTd.getFieldName(i);
        }
        return new TupleDesc(fieldTypes, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        if (hasNext()) {
            return iterator.next();
        } else {
            throw new NoSuchElementException("No more tuples in SeqScan");
        }
    }

    public void close() {
        iterator = null; 
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        if (iterator != null) {
            iterator.close(); 
            iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid); // Obtenir un nouvel itérateur
        } else {
            throw new DbException("Iterator is null. Cannot rewind.");
        }
    }
}
