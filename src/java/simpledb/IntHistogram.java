package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int totalCount;
    private int[] bucketCounts;
    private int bucketRange;

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
        this.totalCount = 0;
        this.bucketCounts = new int[buckets];
        this.bucketRange = (int) Math.ceil((max - min + 1) / (double) buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int bucketIndex;
        if (v == max) {
            bucketIndex = buckets - 1;
        } else {
            bucketIndex = (v - min) / bucketRange;
        }
        bucketCounts[bucketIndex]++;
        totalCount++;
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

    	if (totalCount == 0) {
            return 0.0;
        }
        double selectivity = 0.0;
        switch (op) {
            case EQUALS:
                selectivity = estimateEquals(v);
                break;
            case NOT_EQUALS:
                selectivity = 1.0 - estimateEquals(v);
                break;
            case GREATER_THAN:
                selectivity = estimateGreaterThan(v, false);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateGreaterThan(v, true);
                break;
            case LESS_THAN:
                selectivity = estimateLessThan(v, false);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = estimateLessThan(v, true);
                break;
            default:
                return -1.0;
        }
        return selectivity;
    }

    private double estimateEquals(int v) {
        if (v < min || v > max) {
            return 0.0;
        }
        int bucketIndex = (v == max) ? buckets - 1 : (v - min) / bucketRange;
        int bucketLeft = min + bucketIndex * bucketRange;
        int bucketRight = (bucketIndex == buckets - 1) ? max : bucketLeft + bucketRange - 1;
        int width = bucketRight - bucketLeft + 1;
        double height = (double) bucketCounts[bucketIndex] / width;
        return height / totalCount;
    }

    private double estimateGreaterThan(int v, boolean includeEqual) {
        if (v < min) {
            return 1.0;
        }
        else if (v > max) {
            return 0.0;
        }
        int bucketIndex = (v == max) ? buckets - 1 : (v - min) / bucketRange;
        int bucketLeft = min + bucketIndex * bucketRange;
        int bucketRight = (bucketIndex == buckets - 1) ? max : bucketLeft + bucketRange - 1;
        int width = bucketRight - bucketLeft + 1;
        double bucketSelectivity = 0.0;
        if (v < bucketLeft) {
            bucketSelectivity = bucketCounts[bucketIndex];
        } else {
            int numValuesGreater;
            if (includeEqual) {
                numValuesGreater = bucketRight - v + 1;
            } else {
                numValuesGreater = bucketRight - v;
            }
            bucketSelectivity = (numValuesGreater < 0) ? 0.0 : ((double) bucketCounts[bucketIndex] * numValuesGreater / width);
        }
        
        int countGreaterBuckets = 0;
        for (int i = bucketIndex + 1; i < buckets; i++) {
            countGreaterBuckets += bucketCounts[i];
        }
        double totalFraction = (bucketSelectivity + countGreaterBuckets) / totalCount;
        return totalFraction;
    }

    private double estimateLessThan(int v, boolean includeEqual) {
        if (v > max) {
            return 1.0;
        } else if (v < min) {
            return 0.0;
        }
        int bucketIndex = (v == max) ? buckets - 1 : (v - min) / bucketRange;
        int bucketLeft = min + bucketIndex * bucketRange;
        int bucketRight = (bucketIndex == buckets - 1) ? max : bucketLeft + bucketRange - 1;
        int width = bucketRight - bucketLeft + 1;
        double bucketSelectivity = 0.0;
        if (v > bucketRight) {
            bucketSelectivity = bucketCounts[bucketIndex];
        } else {
            int numValuesLess;
            if (includeEqual) {
                numValuesLess = v - bucketLeft + 1;
            } else {
                numValuesLess = v - bucketLeft;
            }
            bucketSelectivity = (numValuesLess < 0) ? 0.0 : ((double) bucketCounts[bucketIndex] * numValuesLess / width);
        }
        int countLessBuckets = 0;
        for (int i = 0; i < bucketIndex; i++) {
            countLessBuckets += bucketCounts[i];
        }
        double totalFraction = (bucketSelectivity + countLessBuckets) / totalCount;
        return totalFraction;
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
        double sum = 0.0;
        int distinctValues = max - min + 1;
        for (int v = min; v <= max; v++) {
            sum += estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return sum / distinctValues;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String[] bucketStrings = new String[buckets];
        for (int i = 0; i < buckets; i++) {
            int bucketLeft = min + i * bucketRange;
            int bucketRight = (i == buckets - 1) ? max : bucketLeft + bucketRange - 1;
            bucketStrings[i] = "{" + bucketLeft + "-" + bucketRight + ": " + bucketCounts[i] + "}";
        }
        return "IntHistogram: [" + String.join(", ", bucketStrings) + "] Total Count: " + totalCount;
    }
}
