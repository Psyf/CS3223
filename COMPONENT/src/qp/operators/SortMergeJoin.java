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

    Tuple leftTuple;
    Tuple rightTuple;

    boolean isEndOfLeftFile = false;
    boolean isEndOfRightFile = false;

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
        
        sortedLeft = new ExternalSort("Left", left, leftindex, numBuff, 0);
        sortedRight = new ExternalSort("Right", right, rightindex, numBuff, 0);

        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }
        return true;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);

        // If end of left file, add in all the values left from the right
        // as they are bigger then the largest left value.
        if (isEndOfLeftFile) {
            while (!checkRightTupleEmpty() && !outbatch.isFull()) {
                if (getNext) {
                    rightTuple = getNextRightTuple();
                } else {
                    rightTuple = getCurrRightTuple();
                    getNext = true;
                }
                if (!checkRightTupleEmpty()) {
                    outbatch.add(rightTuple);
                } else {
                    break;
                }
            }
            // Since have something in outbatch
            if (!outbatch.isEmpty()) {
                return outbatch;
            } 
            return null; // End of SMJ
        }

        // If end of left file, add in all the values left from the right
        // as they are bigger then the largest left value.
        if (isEndOfRightFile) {
            while (!checkLeftTupleEmpty() && !outbatch.isFull()) {
                if (getNext) {
                    leftTuple = getNextLeftTuple();
                } else {
                    leftTuple = getCurrLeftTuple();
                    getNext = true;
                }
                if (!checkLeftTupleEmpty()) {
                    outbatch.add(leftTuple);
                } else {
                    break;
                }
            }
            // Since have something in outbatch
            if (!outbatch.isEmpty()) {
                return outbatch;
            } 
            return null; // End of SMJ
        }

        if (getNext) {
            leftTuple = getNextLeftTuple();
            // leftTuple = getNextTuple(leftbatch, leftPointer, sortedLeft);
            rightTuple = getNextRightTuple();
        } else {
            leftTuple = getCurrLeftTuple();
            rightTuple = getCurrRightTuple();
            getNext = true;
        }

        ArrayList<Tuple> tempLeftTuples = new ArrayList<>();
        ArrayList<Tuple> tempRightTuples = new ArrayList<>();
        
        while (!outbatch.isFull()) {

            if (checkLeftTupleEmpty() | checkRightTupleEmpty()) break;
            
            // Haven't join finish from the previous batch, continuing joining them
            if (joinPairs.size() > 0) {
                if (startJoinTuples()) {
                    return outbatch; // Batch full, return it first and next will continue joining
                }
            }

            if (checkLeftTupleEmpty() | checkRightTupleEmpty()) break;
            
            int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
            // FOR DEBUGGING: (Uncomment)
            // System.out.println("Result" + compareResult);
            // Debug.PPrint(leftTuple);
            // Debug.PPrint(rightTuple);

            if (compareResult == 0) {
                tempLeftTuples.add(leftTuple);
                tempRightTuples.add(rightTuple);

                // Left can join
                Tuple previousLeftTuple = leftTuple;
                leftTuple = getNextLeftTuple();
                checkLeftTupleEmpty();
                while (leftTuple != null && leftTuple.checkJoin(rightTuple, leftindex, rightindex)) {
                    tempLeftTuples.add(leftTuple);
                    previousLeftTuple = leftTuple;
                    leftTuple = getNextLeftTuple();
                    if (checkLeftTupleEmpty()) break;
                }
                
                // Right can join
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
                // leftTuple = getNextTuple(leftbatch, leftPointer, sortedLeft);
                if (leftTuple == null) break;
            } else if (compareResult > 0) {
                rightTuple = getNextRightTuple();
                if (rightTuple == null) break;
            }
        }
        return outbatch;
    }

    private Tuple getCurrLeftTuple() {
        return leftbatch.get(leftPointer - 1); 
    }

    private Tuple getNextTuple(Batch batch, int pointer, ExternalSort sortedFile) {
        
        // Reads a new batch if neccessary
        if (batch == null || pointer == batch.size()) {
            batch = sortedFile.next();
            pointer = 0;
        }
        // Ensures that the left batch still has tuples left
        if (batch == null) {
            return null;
        }
        return batch.get(pointer++); 
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
            isEndOfRightFile = true;
            return true;
        }
        return false;
    }

    private boolean checkLeftTupleEmpty() {
        if (leftTuple == null) {
            isEndOfLeftFile = true;
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
        // System.out.println("Ran here");
        for (int i = 0; i < joinPairs.size(); i++) {
            TuplePair joinPair = joinPairs.get(0);
            Tuple outtuple = joinPair.joinTuple();
            outbatch.add(outtuple);
            joinPairs.remove(0);
            if (outbatch.isFull()) {
                // Outbatch is full
                return true;
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
