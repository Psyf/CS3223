/**
 * Page Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.BatchList;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightPage;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int leftBlockCur;                      // Cursor for left side buffer
    int rightPageCur;                      // Cursor for right side buffer
    boolean doneReadingLeftFile;                   // Whether end of stream (left table) is reached
    boolean doneReadingRightFile;                   // Whether end of stream (right table) is reached
    
    BatchList leftBlock;            // Represents num of tuples in buffer

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** initialise the batchlist used for the join */
        leftBlock = new BatchList(tuplesize, numBuff-2);
        //System.out.printf("Batchlist max size: %d, NumBuff: %d, PageSize: %d, TupleSize: %d\n", batchlist.getMaxSize(), numBuff, Batch.getPageSize(), tuplesize);

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        leftBlockCur = 0;
        rightPageCur = 0;
        doneReadingLeftFile = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        doneReadingRightFile = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() { 
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {

            if (doneReadingLeftFile && doneReadingRightFile) {
                if (!outbatch.isEmpty()) return outbatch; 
                else return null;
            }

            if (doneReadingRightFile == true) { 
                // means we need a new Block from LeftFile
                /** reset batchlist */
                leftBlock.clear();

                /** new batchlist needs to be prepared by fetching left pages **/
                while(!leftBlock.isFull()) {
                    leftbatch = left.next();        // fetch a new page -> refer to next() in Scan.java
                    if (leftbatch == null) {        // no more left pages to be fetched! 
                        doneReadingLeftFile = true;
                        break; 
                    } else {
                        leftBlock.addBatch(leftbatch);
                    }
                }
                /** Whenever a new left Block came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    doneReadingRightFile = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            while (doneReadingRightFile == false || rightPageCur != 0) {
                if (rightPageCur == 0 && leftBlockCur == 0) {
                    try {
                        rightPage = (Batch) in.readObject();
                    } catch (EOFException e) {
                        doneReadingRightFile = true;
                        try { in.close(); break; } 
                        catch (IOException io) { System.out.println("BlockNestedJoin: Error in reading temporary file"); } 
                    } catch (ClassNotFoundException c) {
                        System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                        System.exit(1);
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                        System.exit(1);
                    }
                }
                for (; leftBlockCur < leftBlock.size(); leftBlockCur++) {
                    Tuple lefttuple = leftBlock.get(leftBlockCur);
                    for (; rightPageCur < rightPage.size(); rightPageCur++) {
                        Tuple righttuple = rightPage.get(rightPageCur);
                        if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                            Tuple outtuple = lefttuple.joinWith(righttuple);
                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                rightPageCur++;
                                return outbatch;
                            }
                        }
                    }
                    rightPageCur = 0;
                }
                rightPageCur = 0;
                leftBlockCur = 0; 
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        this.getLeft().close(); 
        this.getRight().close(); 
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
