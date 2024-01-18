    package simpledb;

    import java.util.ArrayList;

    /**
     * Knows how to compute some aggregate over a set of IntFields.
     */
    public class IntegerAggregator implements Aggregator {

        private static final long serialVersionUID = 1L;
        
        private int gbfield;
        private int afield;
        private Type gbfieldtype;
        private Op what;
        
        /*
        * ArrayList of tuples of the type (group_field, aggregate_value)
        * that will be used to perform grouping and aggregates
        */
        private ArrayList<Tuple> groupedAggregatesTuples;
        /*
        * TupleDesc associated with the previous ArrayList of tuples
        */
        private TupleDesc grAggrTd;
        
        /*
        * ArrayList of tuples that will contain only one tuple with one field 
        * corresponding to the aggregate specified in the constructor
        * (used in the case that no grouping is applied)
        */
        private ArrayList<Tuple> uniqueAggregateTuple;
        /*
        * TupleDesc associated with the tuple in the single-element ArrayList above
        */
        private TupleDesc uniqueTd;
        
        /*
        * ArrayList of counters that will keep count of the number of elements per group.
        * (if grouping is not applied, it will contain as its only element the value 
        * of the total number of elements aggregated) 
        */
        private ArrayList<Integer> counts;
        
        /*
        * ArrayList of sums that will keep count of the sums of the elements per group.
        * (if grouping is not applied, it will contain as its only element the sum 
        * of all the elements aggregated) 
        */
        private ArrayList<Integer> sums;
        
        /*
        * Return the index of the tuple, in a given ArrayList of tuples, whose first field (the group) corresponds
        * to the specified group.
        * Returns -1 if the group is not found 
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
            this.gbfield = gbfield;
            this.gbfieldtype = gbfieldtype;
            this.afield = afield;
            this.what = what;
            
            counts = new ArrayList<>();
            sums = new ArrayList<>();
            
            // for grouping, initialize the ArrayList groupedAggregatesTuple
            // and its TupleDesc
            if (gbfield != NO_GROUPING) {
                Type[] grAggrTypes = {this.gbfieldtype, Type.INT_TYPE};
                grAggrTd = new TupleDesc(grAggrTypes);
                groupedAggregatesTuples = new ArrayList<Tuple>();
            } else {
                // for no grouping, initialize the single-element ArrayList
                // uniqueAggregateTuple and its associated TupleDesc
                Type[] uniqueAggrType = {Type.INT_TYPE};
                uniqueTd = new TupleDesc(uniqueAggrType);
                uniqueAggregateTuple = new ArrayList<Tuple>();
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
            // Get the IntField to merge into the aggregate from the new tuple
            IntField newTupValue = (IntField) tup.getField(afield);

            if (gbfield != NO_GROUPING) { 
                Tuple grouptup;

                // Determine if the group that this new tuple belongs to already exists and updates
                int groupIndex = findGroup(tup.getField(gbfield), groupedAggregatesTuples);
                if (groupIndex != -1) { 
                    counts.set(groupIndex, counts.get(groupIndex) + 1);
                    sums.set(groupIndex, sums.get(groupIndex) + newTupValue.getValue());
                    grouptup = groupedAggregatesTuples.get(groupIndex);

                    switch (what) {
                        case MIN:
                            // sotre the new value if it is the new minimum for this group
                            if (newTupValue.compare(Predicate.Op.LESS_THAN, grouptup.getField(1))) {
                                grouptup.setField(1, newTupValue);
                            }
                            break;
                        case MAX:
                            // same for max
                            if (newTupValue.compare(Predicate.Op.GREATER_THAN, grouptup.getField(1))) {
                                grouptup.setField(1, newTupValue);
                            }
                            break;
                        case SUM:
                            // Add the new value to the aggregate 
                            IntField sum = new IntField(((IntField) grouptup.getField(1)).getValue() + newTupValue.getValue());
                            grouptup.setField(1, sum);
                            break;
                        case AVG:
                            // Update the aggregated average
                            int newavg = sums.get(groupIndex) / counts.get(groupIndex);
                            grouptup.setField(1, new IntField(newavg));
                            break;
                        case COUNT:
                            int updatedcount = counts.get(groupIndex);
                            grouptup.setField(1, new IntField(updatedcount));
                            break;
                        default:
                            break;
                    }

                    groupedAggregatesTuples.set(groupIndex, grouptup);
                } else { 
                    counts.add(1);
                    sums.add(newTupValue.getValue());
                    grouptup = new Tuple(grAggrTd);
                    grouptup.setField(0, tup.getField(gbfield));

                    switch (what) {
                        case MIN:
                        case MAX:
                        case SUM:
                        case AVG:
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
            } else { 
                if (counts.size() == 0) {
                    counts.add(0);
                    sums.add(0);
                }
                counts.set(0, counts.get(0) + 1);
                sums.set(0, sums.get(0) + newTupValue.getValue());

                switch (what) {
                    case MIN:
                        // store if first tuple
                        if (uniqueAggregateTuple.isEmpty()) {
                            Tuple firsttup = new Tuple(uniqueTd);
                            firsttup.setField(0, newTupValue);
                            uniqueAggregateTuple.add(firsttup);
                        } else {
                            // Store if min
                            if (newTupValue.compare(Predicate.Op.LESS_THAN, uniqueAggregateTuple.get(0).getField(0))) {
                                Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                                prevUniqueAggrTuple.setField(0, newTupValue);
                                uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
                            }
                        }
                        break;
                    case MAX:
                        // store if first
                        if (uniqueAggregateTuple.isEmpty()) {
                            Tuple firsttup = new Tuple(uniqueTd);
                            firsttup.setField(0, newTupValue);
                            uniqueAggregateTuple.add(firsttup);
                        } else {
                            // Store if max
                            if (newTupValue.compare(Predicate.Op.GREATER_THAN, uniqueAggregateTuple.get(0).getField(0))) {
                                Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                                prevUniqueAggrTuple.setField(0, newTupValue);
                                uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
                            }
                        }
                        break;
                    case SUM:
                        // store if first
                        if (uniqueAggregateTuple.isEmpty()) {
                            Tuple firsttup = new Tuple(uniqueTd);
                            firsttup.setField(0, newTupValue);
                            uniqueAggregateTuple.add(firsttup);
                        } else {
                            // Add the new value to the aggregate
                            Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                            IntField sum = new IntField(((IntField) prevUniqueAggrTuple.getField(0)).getValue() + newTupValue.getValue());
                            prevUniqueAggrTuple.setField(0, sum);
                            uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
                        }
                        break;
                    case AVG:
                        // store if first
                        if (uniqueAggregateTuple.isEmpty()) {
                            Tuple firsttup = new Tuple(uniqueTd);
                            firsttup.setField(0, newTupValue);
                            uniqueAggregateTuple.add(firsttup);
                        } else {
                            // update the aggregated average
                            Tuple prevUniqueAggrTuple = uniqueAggregateTuple.get(0);
                            int newavg = sums.get(0) / counts.get(0);
                            prevUniqueAggrTuple.setField(0, new IntField(newavg));
                            uniqueAggregateTuple.set(0, prevUniqueAggrTuple);
                        }
                        break;
                    case COUNT:
                        // If first tuple in the aggregate, initialize it 
                        if (uniqueAggregateTuple.isEmpty()) {
                            Tuple firsttup = new Tuple(uniqueTd);
                            firsttup.setField(0, new IntField(0));
                            uniqueAggregateTuple.add(firsttup);
                        }

                        //increment the aggregate value
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
    
	
