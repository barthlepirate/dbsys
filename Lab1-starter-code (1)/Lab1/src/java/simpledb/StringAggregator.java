package simpledb;
import java.util.ArrayList;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
  
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    // List of tuples of the form (group field, aggregate value) for grouping and aggregations.
    private ArrayList<Tuple> groupedAggregatesTuples;
    
    // TupleDesc associated with groupedAggregatesTuples.
    private TupleDesc grAggrTd;
    
    // List of tuples that contains a single tuple with a field corresponding to the aggregate specified in the constructor (used in the absence of grouping).
    private ArrayList<Tuple> uniqueAggregateTuple;
    
    // TupleDesc associated with uniqueAggregateTuple.
    private TupleDesc uniqueTd;
    
    // List of counters that keeps track of the number of elements per group. If no grouping is applied, it will contain as its only element the value of the total number of elements aggregated.
    private ArrayList<Integer> counts;
    
    /**
     * Returns the index of the tuple in a given list of tuples, whose first field (the group) corresponds to the specified group.
     * Returns -1 if the group is not found.
     */
    private int findGroup(Field group, ArrayList<Tuple> tuples) {
        for (int i = 0; i < tuples.size(); i++) {
            if (group.equals(tuples.get(i).getField(0))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        // Initialize fields
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    
        // Initialize counts list
        counts = new ArrayList<>();
    
        /*
         * Implementation note: the aggregation field will be of type STRING_TYPE if 
         * the aggregation operation is MIN or MAX, while it will be of type INT_TYPE if
         * the aggregation operation is COUNT. 
         */
        Type aggrFieldType = (what == Op.COUNT) ? Type.INT_TYPE : Type.STRING_TYPE;
    
        // Initialize groupedAggregatesTuples and grAggrTd if grouping is required
        if (gbfield != NO_GROUPING) {
            Type[] grAggrTypes = {this.gbfieldtype, aggrFieldType};
            grAggrTd = new TupleDesc(grAggrTypes);
            groupedAggregatesTuples = new ArrayList<>();
        } else {
            // Initialize uniqueAggregateTuple and uniqueTd if no grouping is required
            Type[] uniqueAggrType = {aggrFieldType};
            uniqueTd = new TupleDesc(uniqueAggrType);
            uniqueAggregateTuple = new ArrayList<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // Get the StringField to merge into the aggregate from the new tuple
        StringField newTupValue = (StringField) tup.getField(afield);
        
        if (gbfield != NO_GROUPING) { // grouping
            Tuple grouptup;
        
            // Check if the group to which this new tuple belongs already exists
            int groupIndex = findGroup(tup.getField(gbfield), groupedAggregatesTuples);
            if (groupIndex != -1) { // existing group 
                // Update the count of this group
                counts.set(groupIndex, counts.get(groupIndex) + 1);
                // Get the tuple for this group from groupedAggregatesTuples
                grouptup = groupedAggregatesTuples.get(groupIndex);
        
                switch (what) {
                case MIN:
                    // If the new value to aggregate in tup is the new minimum for this group, store it
                    if (newTupValue.compare(Predicate.Op.LESS_THAN, grouptup.getField(1))) {
                        grouptup.setField(1, newTupValue);
                    }
                    break;
                case MAX:
                    // If the new value to aggregate in tup is the new maximum for this group, store it
                    if (newTupValue.compare(Predicate.Op.GREATER_THAN, grouptup.getField(1))) {
                        grouptup.setField(1, newTupValue);
                    }
                    break;
                case COUNT:
                    int updatedcount = counts.get(groupIndex);
                    grouptup.setField(1, new IntField(updatedcount));
                    break;
                default:
                    break;
                }
        
                groupedAggregatesTuples.set(groupIndex, grouptup);
            } else { // new group
                // Add a new element to the counts arraylist for this group and set it to 1
                counts.add(1);
                // Add a new tuple for this group in groupedAggregatesTuples
                grouptup = new Tuple(grAggrTd);
                // Set the group value for this group in its associated tuple
                grouptup.setField(0, tup.getField(gbfield));
        
                switch (what) {
                case MIN:
                case MAX:
                    grouptup.setField(1, newTupValue);
                    break;
                case COUNT:
                    grouptup.setField(1, new IntField(1));
                    break;
                default:
                    break;
                }
        
                groupedAggregatesTuples.add(grouptup);
            }
        } else { // No grouping
            // Update the unique count (needed for both the COUNT aggregation operation
            // and to update the average in the AVG aggregation operation
            if (counts.size() == 0) {
                counts.add(0);
            }
            counts.set(0, counts.get(0) + 1);
        
            switch (what) {
            case MIN:
                // If it is the first tuple in the aggregate, store it
                if (uniqueAggregateTuple.isEmpty()) {
                    Tuple firsttup = new Tuple(uniqueTd);
                    firsttup.setField(0, newTupValue);
                    uniqueAggregateTuple.add(firsttup);
                } else {
                    // If the new value to aggregate in tup is the new minimum, store it
                    if (newTupValue.compare(Predicate.Op.LESS_THAN, uniqueAggregateTuple.get(0).getField(0))) {
                        Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                        prevUniqueAggrTuple.setField(0, newTupValue);
                        uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
                    }
                }
                break;
            case MAX:
                // If it is the first tuple in the aggregate, store it
                if (uniqueAggregateTuple.isEmpty()) {
                    Tuple firsttup = new Tuple(uniqueTd);
                    firsttup.setField(0, newTupValue);
                    uniqueAggregateTuple.add(firsttup);
                } else {
                    // If the new value to aggregate in tup is the new maximum, store it
                    if (newTupValue.compare(Predicate.Op.GREATER_THAN, uniqueAggregateTuple.get(0).getField(0))) {
                        Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                        prevUniqueAggrTuple.setField(0, newTupValue);
                        uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
                    }
                }
                break;
            case COUNT:
                // If it is the first tuple in the aggregate, initialize it to 0
                if (uniqueAggregateTuple.isEmpty()) {
                    Tuple firsttup = new Tuple(uniqueTd);
                    firsttup.setField(0, new IntField(0));
                    uniqueAggregateTuple.add(firsttup);
                }
        
                // Then, increment the aggregate value by 1
                Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                int updatedcount = counts.get(0);
                prevUniqueAggrTuple.setField(0, new IntField(updatedcount));
                uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
        
                break;
            default:
                break;
            }
        }
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
        // Generate an iterator for tuples
        TupleIterator tupleIterator;
    
        if (gbfield != NO_GROUPING) {
            // If grouping is applied, use TupleIterator with the groupedAggregatesTuples and associated TupleDesc
            tupleIterator = new TupleIterator(grAggrTd, groupedAggregatesTuples);
        } else {
            // If no grouping is applied, use TupleIterator with the uniqueAggregateTuple and associated TupleDesc
            tupleIterator = new TupleIterator(uniqueTd, uniqueAggregateTuple);
        }
    
        return tupleIterator;
    }

}
