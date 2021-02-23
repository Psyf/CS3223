/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.AggregateValue;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Aggregate extends Operator {

    Operator base;                 // Base table to project
    int batchsize;                 // Number of tuples per outbatch

    Batch inbatch;
    Batch outbatch;

    AggregateValue result;

    public Aggregate(Operator base, int type) {
        super(type);
        this.base = base;
        result = new AggregateValue(type); 
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

        while (inbatch != null) {
            for (int i = 0; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.get(i);
                result.record((int) basetuple.dataAt(0));
            }
            inbatch = base.next(); 
        }

        ArrayList<Object> data = new ArrayList<>();
        data.add(result.get());
        Tuple tup = new Tuple(data); 
        outbatch.add(tup);
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
        Aggregate newAgg = new Aggregate(newbase, optype);
        Schema newSchema = newbase.getSchema();
        newAgg.setSchema(newSchema);
        return newAgg;
    }

}
