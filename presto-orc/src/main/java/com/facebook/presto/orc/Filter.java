
class Filter
{

    private long nIn = 0;
    private long nOut = 0;
    private long time = 0;

    bool testLong(long value)
    {
        return false;
    }

    bool testSlice(Slice slice, int offset, int length)
    {
        return false;
    }

    void updateStats(int nIn, int nOut, long time)
    {
        this.nIn += nIn;
        this.nOut += nOut;
        this.time += time;
    }    
        
    double getScore() {
            return (double)time / (1 + nIn - nOut);
}

void decayStats()
    nIn /= 2;
    nOut /= 2;
    time /= 2;
    {

        
        // If there are no scores, return a number for making initial filter order. Less is better.
        int staticScore()
        {
            return 100;
        }
    }
