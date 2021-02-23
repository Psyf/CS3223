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

    ArrayList<TuplePair> joinPairs = new ArrayList<>();

    int leftPointer = 0;
    int rightPointer = 0;

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
        
        sortedLeft = new ExternalSort("Left", left, leftindex, numBuff);
        sortedRight = new ExternalSort("Right", right, rightindex, numBuff);

        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }
        return true;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);
        
        // if (leftbatch == null) {
        //     leftbatch = sortedLeft.next();
        //     if (leftbatch == null) {
        //         // EOF
        //         return null;
        //     }
        // }
        // if (rightbatch == null) {
        //     rightbatch = sortedRight.next();
        //     if (rightbatch == null) {
        //         // EOF
        //         return null;
        //     }
        // }
        // leftbatch = sortedLeft.next();
        // rightbatch = sortedRight.next();
        Tuple leftTuple = getNextLeftTuple();
        Tuple rightTuple = getNextRightTuple();

        if (isEndOfFile) {
            //EOF
            System.out.println("Done");
            return null;
        }

        ArrayList<Tuple> tempLeftTuples = new ArrayList<>();
        ArrayList<Tuple> tempRightTuples = new ArrayList<>();
        
        while (!outbatch.isFull()) {

            if (leftTuple == null | rightTuple == null) {
                isEndOfFile = true;
                break;
            }
            // System.out.println(leftTuple);
            // System.out.println(rightTuple);
            
            // Haven't join finish from the previous batch, continuing joining them
            if (joinPairs.size() > 0) {
                if (startJoinTuples()) {
                    return outbatch; // Batch full, return it first and next will continue joining
                }
            }
            int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
            System.out.println(compareResult);

            //TODO: Change back
            // joinPairs.add(new TuplePair(leftTuple, rightTuple));
            // startJoinTuples();

            if (compareResult == 0) {
                System.out.println("Equal");
                tempLeftTuples.add(leftTuple);
                tempRightTuples.add(rightTuple);

                // Left can join
                int i = 1;
                while (leftbatch.size() > i && leftbatch.get(i).checkJoin(rightTuple, leftindex, rightindex)) {
                    leftTuple = getNextLeftTuple();
                    if (leftTuple == null) break;
                    tempLeftTuples.add(leftTuple);
                    i++;
                }

                // Right can join
                int j = 1;
                while (rightbatch.size() > i && rightbatch.get(j).checkJoin(leftTuple, leftindex, rightindex)) {
                    rightTuple = getNextRightTuple();
                    if (rightTuple == null) break;
                    tempRightTuples.add(rightTuple);
                    j++;
                }

                // At the end, add all these pairs as join pairs join all those in temp left and temp right
                for (Tuple iTuple: tempLeftTuples) {
                    for (Tuple jTuple: tempRightTuples) {
                        joinPairs.add(new TuplePair(iTuple, jTuple));
                    }
                }

                System.out.println("Finish having join pairs");

                // Join them together and return the outbatch
                startJoinTuples();
                return outbatch;
            
            } else if (compareResult < 0) {
                leftTuple = getNextLeftTuple();
                if (leftTuple == null) break;
            } else if (compareResult > 0) {
                rightTuple = getNextRightTuple();
                if (rightTuple == null) break;
            }
        }
        return outbatch;
    }

    private Tuple getNextLeftTuple() {
        
        // Reads a new batch if neccessary
        if (leftbatch == null ||leftPointer == leftbatch.size()) {
            leftbatch = sortedLeft.next();
            leftPointer = 0;
        }
        // Ensures that the left batch still has tuples left
        if (leftbatch == null) {
            return null;
        }
        return leftbatch.get(leftPointer++); 
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
        System.out.println("Ranned here");
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
