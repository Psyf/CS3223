/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    Object[] aggVals;
    Boolean isAgg = null; 

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        aggVals = new Object[attrset.size()]; 
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            // Make sure all of em are agg, or none of em are agg
            if (isAgg == null) {
                isAgg = !(attr.getAggType() == Attribute.NONE);
            } else if (isAgg != (!(attr.getAggType() == Attribute.NONE)) ) {
                System.out.println("Cannot mix Aggregate Operators with others!");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (inbatch == null) {
            if (isAgg) {
                ArrayList<Object> result = new ArrayList<>();
                for (int i=0; i<attrset.size(); i++) {
                    result.add(aggVals[i]);
                }
                Tuple outtuple = new Tuple(result);
                outbatch.add(outtuple);
                isAgg = false; // hack: so that next iteration, it exits
                return outbatch;
            } else {
                return null;
            }
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            //Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                if (attrset.get(j).getAggType() == Attribute.MIN) {
                    if (aggVals[j] == null) {
                        aggVals[j] = data;
                    }
                    else {
                        aggVals[j] = Math.min((int) aggVals[j], (int) data);
                    }
                } else if (attrset.get(j).getAggType() == Attribute.MAX) {
                    if (aggVals[j] == null) {
                        aggVals[j] = data;
                    }
                    else {
                        aggVals[j] = Math.max((int) aggVals[j], (int) data);
                    }
                } else if (attrset.get(j).getAggType() == Attribute.COUNT) {
                    if (aggVals[j] == null) {
                        aggVals[j] = 1;
                    }
                    else {
                        aggVals[j] = (int) aggVals[j] + 1;
                    }
                } else if (attrset.get(j).getAggType() == Attribute.AVG) {
                    // TODO 
                }
                else {
                    present.add(data);
                }
            }
            if (!isAgg) {
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);
            }
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
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
