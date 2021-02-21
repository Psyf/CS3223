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

    int leftIndex = 0;
    int rightIndex = 0;

    ArrayList<TuplePair> joinPairs = new ArrayList<>();

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
        
        sortedLeft = new ExternalSort(left, leftindex, numBuff);
        sortedRight = new ExternalSort(right, rightindex, numBuff);

        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }
        return true;
    }

    public Batch next() {
        outbatch = new Batch(batchsize);

        leftbatch = (Batch) sortedLeft.next();
        rightbatch = (Batch) sortedRight.next();
        
        // Advance sorted left value until left's sort key >= current right sort key 
        Tuple leftTuple = leftbatch.remove(0);
        Tuple rightTuple = rightbatch.remove(0);

        ArrayList<Tuple> tempLeftTuples = new ArrayList<>();
        ArrayList<Tuple> tempRightTuples = new ArrayList<>();
        
        while (sortedLeft != null && sortedRight != null) {

            if (outbatch.isFull()) {
                return outbatch;
            }
            
            // Haven't join finish from the previous batch, continuing joining them
            if (joinPairs.size() > 0) {
                if (startJoinTuples() != null) {
                    return outbatch; // Batch full, return it first
                }
            }

            int compareResult = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);

            if (compareResult == 0) {
                tempLeftTuples.add(leftTuple);
                tempRightTuples.add(rightTuple);

                // Left can join
                int i = 1;
                while (leftbatch.get(i).checkJoin(rightTuple, leftindex, rightindex)) {
                    leftTuple = getNextLeftTuple();
                    if (leftTuple == null) break;
                    tempLeftTuples.add(leftTuple);
                    i++;
                }

                // Right can join
                int j = 1;
                while (rightbatch.get(j).checkJoin(leftTuple, leftindex, rightindex)) {
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

                // Join them together
                if (startJoinTuples() != null) {
                    return outbatch;
                }
                
            } else if (compareResult < 0) {
                leftTuple = getNextLeftTuple();
            } else if (compareResult > 0) {
                rightTuple = getNextRightTuple();
            }
        }
        return outbatch;
    }

    private Tuple getNextLeftTuple() {
        // Reads a new batch if neccessary
        if (leftbatch.isEmpty()) {
            leftbatch = (Batch) sortedLeft.next();
        }
        // Ensures that the left batch still has tuples left
        if (leftbatch == null) {
            return null;
        }
        return leftbatch.remove(0); // Have a pointer instead of remove to improve 
    }

    private Tuple getNextRightTuple() {
        // Reads a new batch if neccessary
        if (rightbatch.isEmpty()) {
            rightbatch = (Batch) sortedRight.next();
        }
        // Ensures that the right batch still has tuples left
        if (rightbatch == null) {
            return null;
        }
        return rightbatch.remove(0);
    }

    private Batch startJoinTuples() {
        for (TuplePair joinPair: joinPairs) {
            Tuple outtuple = joinPair.joinTuple();
            joinPairs.remove(joinPair);
            outbatch.add(outtuple);
            if (outbatch.isFull()) {
                return outbatch;
            }
        }
        return null;
    }

    public boolean close() {
        if (sortedLeft.close() && sortedRight.close()) {
            return true;
        }
        return false;
    }
}