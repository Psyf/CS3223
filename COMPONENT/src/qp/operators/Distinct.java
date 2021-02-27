package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;
import java.util.ArrayList;

public class Distinct extends Operator {

    Operator base;                 // Base table to project
    Operator sortedBase; 

    int batchsize;                 // Number of tuples per outbatch

    Batch inbatch;
    Batch outbatch;

    Tuple prevTuple; 
    boolean done = false; 

    int numBuffers; 

    int partitionPointer = 0; 

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumBuff(int numBuff) {
        this.numBuffers = numBuff; 
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize(); 
        batchsize = Batch.getPageSize() / tuplesize; 

        ArrayList<Integer> sortIndices = new ArrayList<Integer>(); 
        for (int i = 0; i < schema.getNumCols(); i++) { sortIndices.add(i); }
       
        sortedBase = new ExternalSort("Distinct", this.base, sortIndices, this.numBuffers, 0); 

        if (!sortedBase.open()) return false;
        else { return true; }
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        //System.out.println("CALLING NEXT() ON DISTINCT"); 
        if (done == true) { return null; }
        else {
            outbatch = new Batch(this.batchsize); 
            while (!outbatch.isFull()) {
                inbatch = sortedBase.next();
                if (inbatch == null) { 
                    done = true; 
                    break; 
                } else {
                    for (int i = 0; i < inbatch.size(); i++) {
                        if (prevTuple == null) { 
                            prevTuple = inbatch.get(i); 
                            outbatch.add(prevTuple); 
                        }
                        else {
                            if (!prevTuple.equals(inbatch.get(i))) {
                                prevTuple = inbatch.get(i);
                                outbatch.add(prevTuple);
                            }
                        }
                    }
                } 
            }
            return outbatch;
        }
    }

    /**
     * Close the operator
     */
    public boolean close()  {
        inbatch = null;
        outbatch = null;
        sortedBase.close(); 
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newdist = new Distinct(newbase, optype);
        newdist.setSchema(newbase.getSchema());
        return newdist;
    }
}
