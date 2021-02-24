package qp.operators;

import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;
import qp.utils.TuplePair;

public class SortMergeJoin extends Join  {

    ExternalSort sortedLeft;
    ExternalSort sortedRight;

    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream

    boolean getNext = true;         // Represents that the next value has not been gotten yet 

    ArrayList<TuplePair> joinPairs = new ArrayList<>();

    int leftPointer = 0;
    int rightPointer = 0;
    int joinPointer = 0;

    Tuple leftTuple;
    Tuple rightTuple;

    boolean isEndOfFile = false;

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
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        
        // Get sorted left and right file using external sort
        sortedLeft = new ExternalSort("Left", left, leftindex, numBuff, 0);
        sortedRight = new ExternalSort("Right", right, rightindex, numBuff, 0);

        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }
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
        
        while (!outbatch.isFull()) {

            if (checkLeftTupleEmpty() | checkRightTupleEmpty()) break;
            
            // Haven't join finish from the previous batch, continuing joining them
            if (joinPairs.size() > 0) {
                if (startJoinTuples()) {
                    return outbatch; // Batch full, return it first and next will continue joining
                }
                if (checkLeftTupleEmpty() | checkRightTupleEmpty()) break;
            }
            
            int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
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
                checkLeftTupleEmpty();
                
                while (leftTuple != null && leftTuple.checkJoin(rightTuple, leftindex, rightindex)) {
                    tempLeftTuples.add(leftTuple);
                    previousLeftTuple = leftTuple;
                    leftTuple = getNextLeftTuple();
                    if (checkLeftTupleEmpty()) break;
                }
                
                // Check if the next right value has the same value
                rightTuple = getNextRightTuple();
                checkRightTupleEmpty();
                
                while (rightTuple != null && rightTuple.checkJoin(previousLeftTuple, rightindex, leftindex)) {
                    tempRightTuples.add(rightTuple);
                    rightTuple = getNextRightTuple();
                    if (checkRightTupleEmpty()) break;
                }

                // At the end, add all these pairs as join pairs join all those in temp left and temp right
                for (Tuple iTuple: tempLeftTuples) {
                    for (Tuple jTuple: tempRightTuples) {
                        joinPairs.add(new TuplePair(iTuple, jTuple));
                    }
                }

                // The left tuple and right tuple has been updated, no need to fetch again
                getNext = false; 

                // Join them together and return the outbatch
                startJoinTuples();
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

    private boolean checkRightTupleEmpty() {
        if (rightTuple == null) {
            isEndOfFile = true;
            return true;
        }
        return false;
    }

    private boolean checkLeftTupleEmpty() {
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


    private boolean startJoinTuples() {
        while(joinPointer < joinPairs.size()) {
            TuplePair joinPair = joinPairs.get(joinPointer);
            Tuple outtuple = joinPair.joinTuple();
            outbatch.add(outtuple);
            joinPointer++;
            
            if (outbatch.isFull()) {
                return true;
            }

            // Reset the tuple and pointer at the last iteration
            if (joinPointer == joinPairs.size()) {
                joinPointer = 0;
                joinPairs.clear();
            }
        }
        return false;
    }

    public boolean close() {
        if (sortedLeft.close() && sortedRight.close()) {
            return true;
        }
        return false;
    }
}
