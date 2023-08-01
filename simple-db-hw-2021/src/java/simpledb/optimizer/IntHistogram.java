package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min;
    private int max;
    private double width;
    private int[] heights;
    private int tupleCount;
    private Set<Integer> uniqueValue;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) * 1.0 / (double) (buckets);
        this.heights = new int[buckets];
        this.tupleCount = 0;
        this.uniqueValue = new HashSet<>();
    }

    private int getIndex(int v) {
        int res = (int) ((v - min) / this.width);
        return res;
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int index = getIndex(v);
        this.heights[index] += 1;
        this.uniqueValue.add(v);
        this.tupleCount += 1;
    }

    private double estimateEqual(int v) {
        if (v < min || v > max) {
            return 0;
        }
        int h = this.heights[getIndex(v)];
        int ntups = 0;
        for (Integer num: heights) {
            ntups += num;
        }
        return (h * 1.0 / ( (int) this.width + 1)) / (ntups * 1.0);
        //return (h * 1.0 / ( (int) this.width)) / (ntups * 1.0);
        //return (h * 1.0 / this.width) / (ntups * 1.0);
    }

    private double estimateGreater(int v) {
        if (v >= max) {
            return 0;
        }
        if (v < min) {
            return 1;
        }
        int index = this.getIndex(v);
        int h_b = this.heights[index];
        int ntups = 0;
        for (Integer num: heights) {
            ntups += num;
        }
        double b_right = (index + 1) * this.width + this.min;
        double b_great_part = (h_b * 1.0 / (ntups * 1.0)) * ((b_right - v * 1.0) / ((int)this.width + 1));

        double tupsGreater = 0;
        for (int i = index + 1; i < buckets; i += 1) {
            tupsGreater += this.heights[i];
        }
        return b_great_part + tupsGreater / ntups;
    }

    private double estimateLess(int v) {
        if (v <= min) {
            return 0;
        }
        if (v > max) {
            return 1;
        }
        int index = this.getIndex(v);
        int h_b = this.heights[index];
        int ntups = 0;
        for (Integer num: heights) {
            ntups += num;
        }
        double b_left = index * this.width + this.min;
        double b_less_part = (h_b * 1.0 / (ntups * 1.0)) * ((v * 1.0 - b_left) / ((int)this.width + 1));

        double tupsLess = 0;
        for (int i = 0; i < index; i += 1) {
            tupsLess += this.heights[i];
        }
        return b_less_part + tupsLess / ntups;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */

    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS: {
                return estimateEqual(v);
            }
            case GREATER_THAN: {
                return estimateGreater(v);
            }
            case GREATER_THAN_OR_EQ: {
                return estimateGreater(v) + estimateEqual(v);
            }
            case LESS_THAN_OR_EQ: {
                return estimateLess(v) + estimateEqual(v);
            }
            case LESS_THAN: {
                return estimateLess(v);
            }
            case NOT_EQUALS: {
                return 1 - estimateEqual(v);
            }
            default:
                throw new IllegalArgumentException("wrong op");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return (double) (1.0 / this.uniqueValue.size());
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("The buckets are ");
        for (Integer num: this.heights) {
            sb.append(num + " ");
        }
        sb.append('\n');
        sb.append("max: " + max + " min: " + min + " width: " + this.width + '\n');
        return sb.toString();
    }
}
