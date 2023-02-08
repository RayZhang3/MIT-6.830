package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;
    private int tableid;
    private int numPages;
    private int ioCostPerPage;
    private int numTuples;
    private DbFile file;
    private TupleDesc td;
    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StringHistogram> stringHistogramMap;
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
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
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.file =  Database.getCatalog().getDatabaseFile(tableid);
        this.numPages = ((HeapFile) file).numPages();
        this.td = file.getTupleDesc();

        int intFieldNum = 0;
        int stringFieldNum = 0;
        for (int i = 0; i < this.td.numFields(); i += 1) {
            if (this.td.getFieldType(i) == Type.INT_TYPE) {
                intFieldNum += 1;
            } else if (this.td.getFieldType(i) == Type.STRING_TYPE) {
                stringFieldNum += 1;
            }
        }
        this.intHistogramMap = new HashMap<>(intFieldNum);
        this.stringHistogramMap = new HashMap<>(stringFieldNum);
        int[] minFieldValue = new int[this.td.numFields()];
        int[] maxFieldValue = new int[this.td.numFields()];
        Arrays.fill(minFieldValue, Integer.MAX_VALUE);
        Arrays.fill(maxFieldValue, Integer.MIN_VALUE);

        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, tableid);

        int tuplesCount = 0;
        try {
            ss1.open();
            while(ss1.hasNext()) {
                Tuple tup = ss1.next();
                tuplesCount += 1;
                for (int i = 0; i < this.td.numFields(); i += 1) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int value = ((IntField)tup.getField(i)).getValue();
                        minFieldValue[i] = Math.min(minFieldValue[i], value);
                        maxFieldValue[i] = Math.max(maxFieldValue[i], value);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.numTuples = tuplesCount;

        for (int i = 0; i < this.td.numFields(); i += 1) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                this.intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, minFieldValue[i], maxFieldValue[i]));
            } else {
                this.stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        try {
            ss1.rewind();
            while(ss1.hasNext()) {
                Tuple tup = ss1.next();
                for (int i = 0; i < this.td.numFields(); i += 1) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int value = ((IntField) tup.getField(i)).getValue();
                        this.intHistogramMap.get(i).addValue(value);
                    } else {
                        String s = ((StringField)tup.getField(i)).getValue();
                        this.stringHistogramMap.get(i).addValue(s);
                    }
                }
            }
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
        return ((HeapFile) file).numPages() * this.ioCostPerPage;
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
        return (int) (this.totalTuples() * selectivityFactor);
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
        if (this.td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram intHistogram = this.intHistogramMap.get(field);
                return intHistogram.avgSelectivity();
        }
         else {
             StringHistogram stringHistogram = this.stringHistogramMap.get(field);
             return stringHistogram.avgSelectivity();
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
        if (this.td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram intHistogram = this.intHistogramMap.get(field);
            return intHistogram.estimateSelectivity(op, ((IntField)constant).getValue());
        }
        else {
            StringHistogram stringHistogram = this.stringHistogramMap.get(field);
            return stringHistogram.estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.numTuples;
    }

}
