package qp.operators;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import qp.utils.Batch;
import qp.utils.Tuple;


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

        // Initialize the buckets
        Batch[] outputBuckets = new Batch[numOutputBuckets];
        for (int i = 0; i < numOutputBuckets; i++) {
            outputBuckets[i] = new Batch(batchsize);
        }

        // Initialize the output streams
        ObjectOutputStream[] outStreams = new ObjectOutputStream[numOutputBuckets]; 
        for (int i = 0; i < numOutputBuckets; i++) {
            String fileName = this.hashCode() + "-Partition-" + i + ".tmp";
            try {
                outStreams[i] = new ObjectOutputStream(new FileOutputStream(fileName));
            } catch (Exception e) {
                System.out.println("Failed to create outStreams!");
                System.exit(1);
            }
        }

        System.out.println("Just before readin..."); 
        this.inbatch = openBase.next(); 
        System.out.println("I can read something at least!"); 

        while (this.inbatch != null) {
            for (int i = 0; i < inbatch.size(); i++) {
                Tuple tup = inbatch.get(i); 
                int candidateBucket = Tuple.hashCode(tup)%numOutputBuckets;
                outputBuckets[candidateBucket].add(tup);
                if (outputBuckets[candidateBucket].isFull()) {
                    try {
                        outStreams[candidateBucket].writeObject(outputBuckets[candidateBucket]);
                    } catch (Exception e) {
                        System.out.println("Failed to write to an outStream!"); 
                    }
                    outputBuckets[candidateBucket] = new Batch(batchsize);
                }
            }
            this.inbatch = base.next(); 
        }

        for (int i=0; i<numOutputBuckets; i++) {
            System.out.println("Writing partitions after end!"); 
            try {
                outStreams[i].writeObject(outputBuckets[i]);
            } catch (Exception e) {
                System.out.println("Failed to write to an outStream!"); 
            }
            outputBuckets[i] = null; 
        }
    }
}
