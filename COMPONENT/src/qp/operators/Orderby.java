/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;
import qp.optimizer.BufferManager;

import java.util.ArrayList;

public class Orderby extends Operator {

    Operator base;                 // Base table to Orderby
    ArrayList<Attribute> attrset;  // Set of attributes to Orderby
    int batchsize;                 // Number of tuples per outbatch

    /**
     * index of the attributes in the base operator
     * * that are to be ordered
     **/
    ArrayList<Integer> attrIndex;
    int direction;
    int numBuffers;
    ExternalSort sortedBase;

    public Orderby(Operator base, ArrayList<Attribute> as, int direction, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.direction = direction;
        this.numBuffers = BufferManager.getNumBuffers();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getOrdAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * ordered from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        
        if (!base.open()) return false;
        
        // System.out.println("Attributes to order by: ");
        // for(int i = 0; i < attrset.size(); i++) {
        //     Debug.PPrint(attrset.get(i));
        // }
        // System.out.println("");

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<Integer>();
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(index);
        }

        sortedBase = new ExternalSort(
            "Orderby", 
            this.base, 
            this.attrIndex, 
            this.numBuffers,
            direction
        );
        // do the external sort
        if (!sortedBase.open()) {
            return false;
        }

        return true;
    }

    /**
     * Read next tuple from the tuple reader pointing to the last sorted run
     */
    public Batch next() {
        Batch outbatch;
        outbatch = sortedBase.next();
        
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        base.close();
        sortedBase.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Orderby newOrderby = new Orderby(newbase, newattr, this.direction, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newOrderby.setSchema(newSchema);
        return newOrderby;
    }

}
