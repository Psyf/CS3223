package qp.operators;

import java.io.File;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

public class SortMergeJoin extends Join  {

    ExternalSort sortedLeft;
    ExternalSort sortedRight;

    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftIndices;   // Indices of the join attributes in left table
    ArrayList<Integer> rightIndices;  // Indices of the join attributes in right table
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream

    boolean getNext = true;         // Represents that the next value has not been gotten yet 

    int leftPointer = 0;
    int rightPointer = 0;
    int joinPointer = 0;

    Tuple leftTuple;
    Tuple rightTuple;

    boolean isEndOfFile = false;

    int tupleClassBatchSize = batchsize > 1 ? batchsize / 2 : 1;

    TupleReader tempJoinPairsReader = new TupleReader(getTempJoinPairsFileName(), tupleClassBatchSize);
    boolean isTempJoinPairsReaderOpen = false;
    TupleWriter tempJoinPairsWriter = new TupleWriter(getTempJoinPairsFileName(), tupleClassBatchSize); // Contains the temp join pairs when the join pair batch is full
    int tempJoinPairsCount = 0;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    @Override
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftIndices = new ArrayList<>();
        rightIndices = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftIndices.add(left.getSchema().indexOf(leftattr));
            rightIndices.add(right.getSchema().indexOf(rightattr));
        }
        
        // Get sorted left and right file using external sort
        sortedLeft = new ExternalSort("Left", left, leftIndices, numBuff, 0);
        sortedRight = new ExternalSort("Right", right, rightIndices, numBuff, 0);

        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }
        return true;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);

        // Haven't join finish from the previous batch, continuing joining them
        if (tempJoinPairsCount > 0) {
            if (!isTempJoinPairsReaderOpen) { 
                tempJoinPairsReader.open(); 
                isTempJoinPairsReaderOpen = true; 
            }
            while (!outbatch.isFull() && tempJoinPairsCount > 0) {
                outbatch.add(tempJoinPairsReader.next());
                tempJoinPairsCount--;
            }
            if (tempJoinPairsCount == 0) { tempJoinPairsReader.close(); isTempJoinPairsReaderOpen = false; } 
        }

        if (outbatch.isFull()) { return outbatch; }

        if (isEndOfFile) {
            return null;
        }

        // When the compare result is equal, we will need to look at the next tuple to know if it is equal
        // As we are looking at the next tuple, we need get current tuple value instead of next tuple
        if (getNext) {
            leftTuple = getNextLeftTuple();
            rightTuple = getNextRightTuple();
        } else {
            leftTuple = getCurrLeftTuple();
            rightTuple = getCurrRightTuple();
            getNext = true;
        }

        
        while (!outbatch.isFull()) {

            if (checkLeftTupleEmptyAndUpdateEOF() | checkRightTupleEmptyAndUpdateEOF()) break;
            
            int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices);
            // FOR DEBUGGING: (Uncomment)
            // System.out.println("Result" + compareResult);
            // Debug.PPrint(leftTuple);
            // Debug.PPrint(rightTuple);

            if (compareResult == 0) {
                // Temp tuples array used for joining
                TupleWriter tempLeftTuplesWriter =  new TupleWriter(getTempLeftTuplesFileName(), tupleClassBatchSize);
                TupleWriter tempRightTuplesWriter = new TupleWriter(getTempRightTuplesFileName(), tupleClassBatchSize);
                tempLeftTuplesWriter.open();
                tempRightTuplesWriter.open();

                // Add them to the temp tuples to be joined later
                tempLeftTuplesWriter.next(leftTuple);
                tempRightTuplesWriter.next(rightTuple);

                // Check if the next left value has the same value
                Tuple previousLeftTuple = leftTuple; // Stores the previous tuple for comparision with the right later 
                leftTuple = getNextLeftTuple();
                checkLeftTupleEmptyAndUpdateEOF();
                
                while (leftTuple != null && leftTuple.checkJoin(rightTuple, leftIndices, rightIndices)) {
                    tempLeftTuplesWriter.next(leftTuple);
                    previousLeftTuple = leftTuple;
                    leftTuple = getNextLeftTuple();
                    if (checkLeftTupleEmptyAndUpdateEOF()) break;
                }
                
                // Check if the next right value has the same value
                rightTuple = getNextRightTuple();
                checkRightTupleEmptyAndUpdateEOF();
                
                while (rightTuple != null && rightTuple.checkJoin(previousLeftTuple, rightIndices, leftIndices)) {
                    tempRightTuplesWriter.next(rightTuple);
                    rightTuple = getNextRightTuple();
                    if (checkRightTupleEmptyAndUpdateEOF()) break;
                }

                tempLeftTuplesWriter.close();
                tempRightTuplesWriter.close();

                // Initialise the tuple readers to read the left tuples and right tuples required for joining
                TupleReader tempLeftTuplesReader =  new TupleReader(getTempLeftTuplesFileName(), tupleClassBatchSize);
                TupleReader tempRightTuplesReader = new TupleReader(getTempRightTuplesFileName(), tupleClassBatchSize);
                tempLeftTuplesReader.open();
                tempRightTuplesReader.open();

                // At the end, add all the join pairs to outbatch, if it is full, add them temporarily to disk
                while (!tempLeftTuplesReader.isEOF()) {
                    Tuple leftTuple = tempLeftTuplesReader.next();
                    
                    while (!tempRightTuplesReader.isEOF()) {
                        Tuple rightTuple = tempRightTuplesReader.next();
                        Tuple joinedTuple = leftTuple.joinWith(rightTuple);
    
                        if (outbatch.isFull()) {
                            // Open the writer if it is writing for the first time
                            if (tempJoinPairsCount == 0) { tempJoinPairsWriter.open(); } 
                            tempJoinPairsWriter.next(joinedTuple);
                            tempJoinPairsCount++;
                            
                        } else {
                            outbatch.add(joinedTuple);
                        }
                    }
                }

                tempLeftTuplesReader.close();
                tempRightTuplesReader.close();
                if (tempJoinPairsCount > 0) { tempJoinPairsWriter.close(); }

                // The left tuple and right tuple has been updated, no need to fetch again
                getNext = false; 
            
            } else if (compareResult < 0) {
                leftTuple = getNextLeftTuple();

            } else if (compareResult > 0) {
                rightTuple = getNextRightTuple();
            }
        }
        return outbatch;
    }

    private Tuple getCurrLeftTuple() {
        return leftbatch.get(leftPointer - 1); 
    }

    private Tuple getNextLeftTuple() {
        
        // Reads a new batch if neccessary
        if (leftbatch == null || leftPointer == leftbatch.size()) {
            leftbatch = sortedLeft.next();
            leftPointer = 0;
        }
        // Ensures that the left batch still has tuples left
        if (leftbatch == null) {
            return null;
        }
        return leftbatch.get(leftPointer++); 
    }

    private boolean checkRightTupleEmptyAndUpdateEOF() {
        if (rightTuple == null) {
            isEndOfFile = true;
            return true;
        }
        return false;
    }

    private boolean checkLeftTupleEmptyAndUpdateEOF() {
        if (leftTuple == null) {
            isEndOfFile = true;
            return true;
        }
        return false;
    }

    private Tuple getCurrRightTuple() {
        return rightbatch.get(rightPointer - 1);
    }

    private Tuple getNextRightTuple() {
        
        // Reads a new batch if neccessary
        if (rightbatch == null ||rightPointer == rightbatch.size()) {
            rightbatch = sortedRight.next();
            rightPointer = 0;
        } 
        // Ensures that the right batch still has tuples left
        if (rightbatch == null) {
            return null;
        }
        return rightbatch.get(rightPointer++); 
    }

    public boolean close() {
        if (!sortedLeft.close() && !sortedRight.close()) {
            return false;
        }
        outbatch.clear();
        outbatch = null;
        leftbatch = null;
        rightbatch = null;
        tempJoinPairsWriter.close();
        tempJoinPairsReader.close();
        cleanupTmpFiles();
        return true;
    }

    public void cleanupTmpFiles() {
        File joinPairFile = new File(getTempJoinPairsFileName()); 
        joinPairFile.delete();
        File leftTuplesFile = new File(getTempLeftTuplesFileName()); 
        leftTuplesFile.delete();
        File rightTuplesFile = new File(getTempRightTuplesFileName()); 
        rightTuplesFile.delete();
    }

    public String getTempJoinPairsFileName() { return "TempJoinPairs.tmp"; }
    public String getTempLeftTuplesFileName() { return "TempLeftTuples.tmp"; }
    public String getTempRightTuplesFileName() { return "TempRightTuples.tmp"; }
}
