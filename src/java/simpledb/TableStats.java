package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int totalTuples;
    private int ioCostPerPage;
    private int numPages;
    private TupleDesc td;
    private IntHistogram[] intHistograms;
    private StringHistogram[] stringHistograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        this.td = file.getTupleDesc();
        numPages = ((HeapFile) file).numPages();
        totalTuples = 0;
        int numFields = td.numFields();
        int[] mins = new int[numFields];
        int[] maxs = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                mins[i] = Integer.MAX_VALUE;
                maxs[i] = Integer.MIN_VALUE;
            }
        }
        
        TransactionId tid = new TransactionId();
        DbFileIterator it = file.iterator(tid);
        try {
            it.open();
            while (it.hasNext()) {
                Tuple tup = it.next();
                totalTuples++;
                for (int i = 0; i < numFields; i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int v = ((IntField) tup.getField(i)).getValue();
                        if (v < mins[i])
                            mins[i] = v;
                        if (v > maxs[i])
                            maxs[i] = v;
                    }
                }
            }
            it.rewind();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        intHistograms = new IntHistogram[numFields];
        stringHistograms = new StringHistogram[numFields];
        for (int i = 0; i < numFields; i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                intHistograms[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
            } else {
                stringHistograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }
        
        try {
            it.rewind();
            while (it.hasNext()) {
                Tuple tup = it.next();
                for (int i = 0; i < numFields; i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int v = ((IntField) tup.getField(i)).getValue();
                        intHistograms[i].addValue(v);
                    } else {
                        String s = ((StringField) tup.getField(i)).getValue();
                        stringHistograms[i].addValue(s);
                    }
                }
            }
            it.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (td.getFieldType(field) == Type.INT_TYPE) {
            return intHistograms[field].avgSelectivity();
        } else {
            return stringHistograms[field].avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (td.getFieldType(field) == Type.INT_TYPE) {
            int v = ((IntField) constant).getValue();
            return intHistograms[field].estimateSelectivity(op, v);
        } else {
            String s = ((StringField) constant).getValue();
            return stringHistograms[field].estimateSelectivity(op, s);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return totalTuples;
    }

}
