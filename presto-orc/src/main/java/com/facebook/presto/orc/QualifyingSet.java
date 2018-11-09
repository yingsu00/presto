
class QualifyingSet
{
    // begin and end define the range of rows coverd. If a row >=
    // begin and < end and is not in positions rangeBegins[i] <= row <
    // rangeEnds[i] then row is not in the qualifying set.
    int begin;
    int end;
    int[] rangeBegins;
    int[] rangeEnds;
    int[] positions;
    int numRanges;
    int numPositions;

    int[] inputNumbers;
    boolean isRanges;
    int size = 0;
    int size()
    {
        return size;
    }

    reset()
    {
        numRanges = 0;
        numPositions = 0;
        size = 0;
    }
    addRange(int begin, int end, int inputNumber)
    {
    }
    
    addPosition(int position, int inputNumber)
    {
        
    }


    
    
    // Sets this to be those rows of input that are above the truncatedRow of output.
    void setContinueAfterTruncate(QualifyingSet input, QualifyingSet truncated)
    {
    }
}

