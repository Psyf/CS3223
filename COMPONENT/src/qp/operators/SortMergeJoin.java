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

    TupleReader tempJoinPairsReader = new TupleReader(getTempJoinPairsFileName(), batchsize);
    TupleWriter tempJoinPairsWriter; // Contains the temp join pairs when the join pair batch is full
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
        // Create a temp tuple writer when join pairs batch overloads
        tempJoinPairsWriter = new TupleWriter(getTempJoinPairsFileName(), batchsize);
        return true;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);

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

        // Temp tuples array used for joining
        ArrayList<Tuple> tempLeftTuples = new ArrayList<>();
        ArrayList<Tuple> tempRightTuples = new ArrayList<>();

        // Haven't join finish from the previous batch, continuing joining them
        if (tempJoinPairsCount > 0) {
            tempJoinPairsReader.open();
            while (!outbatch.isFull() && tempJoinPairsCount > 0) {
                outbatch.add(tempJoinPairsReader.next());
                tempJoinPairsCount--;
            }
            if (tempJoinPairsCount == 0) { tempJoinPairsReader.close(); } 
        }

        if (outbatch.isFull()) { return outbatch; }
        
        while (!outbatch.isFull()) {

            if (checkLeftTupleEmptyAndUpdateEOF() | checkRightTupleEmptyAndUpdateEOF()) break;
            
            int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftIndices, rightIndices);
            // FOR DEBUGGING: (Uncomment)
            // System.out.println("Result" + compareResult);
            // Debug.PPrint(leftTuple);
            // Debug.PPrint(rightTuple);

            if (compareResult == 0) {
                // Add them to the temp tuples to be joined later
                tempLeftTuples.add(leftTuple);
                tempRightTuples.add(rightTuple);

                // Check if the next left value has the same value
                Tuple previousLeftTuple = leftTuple; // Stores the previous tuple for comparision with the right later 
                leftTuple = getNextLeftTuple();
                checkLeftTupleEmptyAndUpdateEOF();
                
                while (leftTuple != null && leftTuple.checkJoin(rightTuple, leftIndices, rightIndices)) {
                    tempLeftTuples.add(leftTuple);
                    previousLeftTuple = leftTuple;
                    leftTuple = getNextLeftTuple();
                    if (checkLeftTupleEmptyAndUpdateEOF()) break;
                }
                
                // Check if the next right value has the same value
                rightTuple = getNextRightTuple();
                checkRightTupleEmptyAndUpdateEOF();
                
                while (rightTuple != null && rightTuple.checkJoin(previousLeftTuple, rightIndices, leftIndices)) {
                    tempRightTuples.add(rightTuple);
                    rightTuple = getNextRightTuple();
                    if (checkRightTupleEmptyAndUpdateEOF()) break;
                }

                // At the end, add all the join pairs to outbatch, if it is full, add them temporarily to disk
                for (Tuple iTuple: tempLeftTuples) {
                    for (Tuple jTuple: tempRightTuples) {
                        Tuple joinedTuple = iTuple.joinWith(jTuple);
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
                if (tempJoinPairsCount > 0) { tempJoinPairsWriter.close(); }
                // System.out.println("Get file");
                // Debug.PPrint(getTempJoinPairsFileName(), batchsize);
                // System.out.println("Out");
                // Debug.PPrint(outbatch);

                // The left tuple and right tuple has been updated, no need to fetch again
                getNext = false; 

                return outbatch;
            
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


    // private boolean startJoinTuples() {
    //     while(joinPointer < joinPairs.size()) {
    //         TuplePair joinPair = joinPairs.get(joinPointer);
    //         Tuple outtuple = joinPair.joinTuple();
    //         outbatch.add(outtuple);
    //         joinPointer++;
            
    //         if (outbatch.isFull()) {
    //             return true;
    //         }

    //         // Reset the tuple and pointer at the last iteration
    //         if (joinPointer == joinPairs.size()) {
    //             joinPointer = 0;
    //             joinPairs.clear();
    //         }
    //     }
    //     return false;
    // }

    public boolean close() {
        if (sortedLeft.close() && sortedRight.close()) {
            return true;
        }
        outbatch.clear();
        outbatch = null;
        leftbatch.clear();
        leftbatch = null;
        rightbatch.clear();
        rightbatch = null;
        tempJoinPairsWriter.close();
        return false;
    }

    public String getTempJoinPairsFileName() {
        String fileName = "TempJoinPairs.tmp";
        return fileName;
    }
}
