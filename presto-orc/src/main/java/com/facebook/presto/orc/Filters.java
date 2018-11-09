

class Filters {
    static public class BigintRange
        extends Filter
    {
        private final long lower;
        private final long upper;

        BigintRange(long lower, lomg upper)
            {
                this.lower = lower;
                this.upper = upper;
            }

        @Override
        bool testLong(long value)
        {
            return value >= lower && value <= upper;
        }
    }

    @Override
    int staticScore()
    {
        // Equality is better than range with both ends, which is better than a range with one end.
        if (upper == lower) {
            return 1;
        }
        return upper != Long.MAX_VALUE && lower != Long.MIN_VALUE : 2 : 3;
    }
}
