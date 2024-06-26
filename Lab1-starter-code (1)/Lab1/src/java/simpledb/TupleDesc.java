package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
	    // Initialize the TDItem with the specified Type and field name.
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
	   //Returns a string representation of the TDItem in the format "fieldName(fieldType)".
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TupleDesc.TDItem> tditems;


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tditems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tditems = new ArrayList<TupleDesc.TDItem>();
        for (int i = 0 ; i< typeAr.length; i++ ){
            TDItem tmpitem = new TDItem(typeAr[i] ,fieldAr[i]);
            tditems.add(tmpitem);
        }


    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        tditems = new ArrayList<TupleDesc.TDItem>();
        for ( int i = 0 ; i< typeAr.length; i++){
            TDItem tmpitem = new TDItem(typeAr[i],"");
            tditems.add(tmpitem);
        }

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tditems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return tditems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return tditems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {

        if (name == null) {
        throw new NoSuchElementException("Field name is null");
        }


        for (int i = 0; i < tditems.size(); i++) {
            TDItem item = tditems.get(i);
            if (name.equals(item.fieldName)) {
                return i;
            }
        }

        throw new NoSuchElementException("Field name not found: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
    	for (int i = 0; i < tditems.size(); i++) {
    		size += tditems.get(i).fieldType.getLen();
		}
        return size;   
         }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int total_lenght = td1.numFields() + td2.numFields();
    	String[] fieldAr = new String[total_lenght];
    	Type[] typeAr = new Type[total_lenght];
     	for (int i = 0; i < total_lenght; i++) {
			if (i < td1.numFields()) {
				typeAr[i] = td1.getFieldType(i);
				fieldAr[i] = td1.getFieldName(i);
			} else {
				typeAr[i] = td2.getFieldType(i - td1.numFields());
				fieldAr[i] = td2.getFieldName(i - td1.numFields());
			}
		}
    	TupleDesc mergedtd = new TupleDesc(typeAr, fieldAr);
        return mergedtd;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o != null && o instanceof TupleDesc) {
    		TupleDesc other = (TupleDesc)o;
        	if (numFields() != other.numFields())
        		return false;
        	for (int i = 0; i < numFields(); i++) {
    			if (!getFieldType(i).equals(other.getFieldType(i)))
    				return false;}
        	return true;
    	} 
    	return false;    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String a = "";
        for (int i = 0; i<numFields();i++){ 
            if (i != 0)
    			a.concat(",");
			a.concat(tditems.get(i).toString());
        }
        return a;
    }
}
