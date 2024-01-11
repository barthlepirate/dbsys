package simpledb;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
  
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    // List of tuples (group_field, aggregate_value) for grouping and aggregates
    private ArrayList<Tuple> groupedAggregatesTuples;
    // TupleDesc associated with the groupedAggregatesTuples list
    private TupleDesc grAggrTd;
    
    // List of tuples with a single field for the aggregate (used without grouping)
    private ArrayList<Tuple> uniqueAggregateTuple;
    // TupleDesc associated with the uniqueAggregateTuple
    private TupleDesc uniqueTd;
    
    // List of counters for the number of elements per group or total count without grouping
    private ArrayList<Integer> counts;
    
    // List of sums for the elements per group or total sum without grouping
    private ArrayList<Integer> sums;
    
    // Returns the index of the tuple in a list with a specified group as its first field
    // Returns -1 if the group is not found
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
        // Initialization of fields
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    
        // Initialization of count and sum lists
        counts = new ArrayList<>();
        sums = new ArrayList<>();
    
        // Initialize groupedAggregatesTuples and grAggrTd if grouping is required
        if (gbfield != NO_GROUPING) {
            Type[] grAggrTypes = {this.gbfieldtype, Type.INT_TYPE};
            grAggrTd = new TupleDesc(grAggrTypes);
            groupedAggregatesTuples = new ArrayList<>();
        } else {
            // Initialize uniqueAggregateTuple and uniqueTd if no grouping is required
            Type[] uniqueAggrType = {Type.INT_TYPE};
            uniqueTd = new TupleDesc(uniqueAggrType);
            uniqueAggregateTuple = new ArrayList<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // Extract the IntField to merge into the aggregate from the new tuple
        IntField newTupleValue = (IntField) tup.getField(afield);
        
        if (gbfield != NO_GROUPING) { // Grouping
            Tuple groupTuple;
        
            // Check if the group that this new tuple belongs to already exists
            int groupIndex = findGroup(tup.getField(gbfield), groupedAggregatesTuples);
        
            if (groupIndex != -1) { // Existing group
                // Update the count and sum of this group
                counts.set(groupIndex, counts.get(groupIndex) + 1);
                sums.set(groupIndex, sums.get(groupIndex) + newTupleValue.getValue());
        
                // Get the tuple for this group from groupedAggregatesTuples
                groupTuple = groupedAggregatesTuples.get(groupIndex);
        
                switch (what) {
                    case MIN:
                    case MAX:
                        // Update if the new value is the new minimum or maximum for this group
                        Field currentGroupValue = groupTuple.getField(1);
                        if (newTupleValue.compare(what.equals(Op.MIN) ? Op.LESS_THAN : Op.GREATER_THAN, currentGroupValue)) {
                            groupTuple.setField(1, newTupleValue);
                        }
                        break;
                    case SUM:
                        // Add the new value to the aggregate for this group
                        IntField currentSum = (IntField) groupTuple.getField(1);
                        IntField newSum = new IntField(currentSum.getValue() + newTupleValue.getValue());
                        groupTuple.setField(1, newSum);
                        break;
                    case AVG:
                        // Update the aggregated average
                        int newAverage = sums.get(groupIndex) / counts.get(groupIndex);
                        groupTuple.setField(1, new IntField(newAverage));
                        break;
                    case COUNT:
                        // Update the count for this group
                        groupTuple.setField(1, new IntField(counts.get(groupIndex)));
                        break;
                    default:
                        break;
                }
        
                groupedAggregatesTuples.set(groupIndex, groupTuple);
            } else { // New group
                // Add a new element to the counts and sums lists for this group
                counts.add(1);
                sums.add(newTupleValue.getValue());
        
                // Add a new tuple for this group in groupedAggregatesTuples
                groupTuple = new Tuple(grAggrTd);
                groupTuple.setField(0, tup.getField(gbfield));
        
                switch (what) {
                    case MIN:
                    case MAX:
                    case SUM:
                    case AVG:
                        groupTuple.setField(1, newTupleValue);
                        break;
                    case COUNT:
                        groupTuple.setField(1, new IntField(1));
                        break;
                    default:
                        break;
                }
        
                groupedAggregatesTuples.add(groupTuple);
            }
        } else { // No grouping
            // Update the unique count (needed for both COUNT and AVG aggregation operations)
            if (counts.isEmpty()) {
                counts.add(0);
                sums.add(0);
            }
        
            counts.set(0, counts.get(0) + 1);
            sums.set(0, sums.get(0) + newTupleValue.getValue());
        
            switch (what) {
                case MIN:
                case MAX:
                    // Update if it is the first tuple or the new value is the new minimum or maximum
                    if (uniqueAggregateTuple.isEmpty() || newTupleValue.compare(what.equals(Op.MIN) ? Op.LESS_THAN : Op.GREATER_THAN, uniqueAggregateTuple.get(0).getField(0))) {
                        Tuple newAggregateTuple = new Tuple(uniqueTd);
                        newAggregateTuple.setField(0, newTupleValue);
                        uniqueAggregateTuple.clear();
                        uniqueAggregateTuple.add(newAggregateTuple);
                    }
                    break;
                case SUM:
                    // If it is the first tuple, store it
                    if (uniqueAggregateTuple.isEmpty()) {
                        Tuple newAggregateTuple = new Tuple(uniqueTd);
                        newAggregateTuple.setField(0, newTupleValue);
                        uniqueAggregateTuple.add(newAggregateTuple);
                    } else {
                        // Add the new value to the aggregate
                        Tuple currentAggregateTuple = uniqueAggregateTuple.get(0);
                        IntField currentSum = (IntField) currentAggregateTuple.getField(0);
                        IntField newSum = new IntField(currentSum.getValue() + newTupleValue.getValue());
                        currentAggregateTuple.setField(0, newSum);
                    }
                    break;
                case AVG:
                    // Update the aggregated average
                    Tuple currentAggregateTuple = uniqueAggregateTuple.get(0);
                    int newAverage = sums.get(0) / counts.get(0);
                    currentAggregateTuple.setField(0, new IntField(newAverage));
                    break;
                case COUNT:
                    // If it is the first tuple, initialize it to 0
                    if (uniqueAggregateTuple.isEmpty()) {
                        Tuple newAggregateTuple = new Tuple(uniqueTd);
                        newAggregateTuple.setField(0, new IntField(0));
                        uniqueAggregateTuple.add(newAggregateTuple);
                    }
        
                    // Increment the count by 1
                    Tuple currentAggregateTuple = uniqueAggregateTuple.get(0);
                    int updatedCount = counts.get(0);
                    currentAggregateTuple.setField(0, new IntField(updatedCount));
                    break;
                default:
                    break;
                }
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
        // Create a TupleIterator based on the grouping requirement
        TupleIterator tupleIterator;
    
        if (gbfield != NO_GROUPING) {
            tupleIterator = new TupleIterator(grAggrTd, groupedAggregatesTuples);
        } else {
            tupleIterator = new TupleIterator(uniqueTd, uniqueAggregateTuple);
        }
    
        return tupleIterator;
    }


}
