package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleWriter;


public class Distinct extends Operator {

    Operator base;                 // Base table to project
    int batchsize;                 // Number of tuples per outbatch

    Batch inbatch;
    Batch outbatch;

    int numOutputBuckets; 

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
        this.numOutputBuckets = 10; //TODO random number
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
        this.partition(base, this.numOutputBuckets, batchsize);

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        // TODO: for each partition, dedup
        
        // TODO: cleanup files

        outbatch = new Batch(batchsize);

        //inbatch = base.next();

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

    public void partition(Operator openBase, int numOutputBuckets, int batchsize) {

        // Initialize the output streams / output buckets
        TupleWriter[] outputPartitions = new TupleWriter[numOutputBuckets]; 
        for (int i = 0; i < numOutputBuckets; i++) {
            String fileName = this.hashCode() + "-Partition-" + i + ".tmp";
            try {
                outputPartitions[i] = new TupleWriter(fileName, batchsize);
                outputPartitions[i].open(); 
            } catch (Exception e) {
                System.out.println("Failed to create outStreams!");
                System.exit(1);
            }
        }

        this.inbatch = openBase.next(); 

        while (this.inbatch != null) {
            for (int i = 0; i < inbatch.size(); i++) {
                Tuple tup = inbatch.get(i); 
                int candidateBucket = hashTuple(tup)%numOutputBuckets;
                Debug.PPrint(tup);
                outputPartitions[candidateBucket].next(tup);
            }
            this.inbatch = base.next(); 
        }

        for (int i=0; i<numOutputBuckets; i++) {
            outputPartitions[i].close();
        }
    }

    public int hashTuple(Tuple tup) {
        int sum = 0; 
        for (Object item : tup.data()) {
            sum += item.toString().hashCode();
        }
        return sum;
    }
}
