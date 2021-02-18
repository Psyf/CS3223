package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;


public class Distinct extends Operator {

    Operator base;                 // Base table to project
    int batchsize;                 // Number of tuples per outbatch

    Batch inbatch;
    Batch outbatch;

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


    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);

        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();

            // TODO: right now does nothing,
            // TODO: need to modify to

            outbatch.add(basetuple);
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newdist = new Distinct(newbase, optype);
        newdist.setSchema(newbase.getSchema());
        return newdist;
    }

}
