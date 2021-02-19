package qp.operators;

import qp.utils.Batch;
import qp.utils.RandNumb;
import qp.utils.Tuple;
import qp.utils.TupleWriter;
import qp.utils.TupleReader;
import java.io.File; 

public class Distinct extends Operator {

    Operator base;                 // Base table to project
    int batchsize;                 // Number of tuples per outbatch

    Batch inbatch;
    Batch outbatch;

    int numBuffers; 

    int partitionPointer = 0; 

    int uid; 

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
        this.uid = RandNumb.randInt(0, 1000000);
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumBuff(int numBuff) {
        System.out.println (numBuff); 
        this.numBuffers = numBuff; 
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        else {
            this.partition(base, this.numBuffers-1, batchsize);
            return true;
        } 

    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {

        // exit if all partitions have been tackled
        if (partitionPointer == this.numBuffers-1) {
            return null;
        }

        // for each partition, dedup
        TupleReader reader = new TupleReader(getTmpFileName(this.partitionPointer), this.batchsize);
        reader.open(); 

        Batch[] slots = new Batch[this.numBuffers-1]; 
        for (int j=0; j<this.numBuffers-1; j++) {
            // Assumption: no slot overflows during probing
            // TODO: make partition recursive
            slots[j] = new Batch(this.batchsize); 
        }
        
        while (!reader.isEOF()) {
            Tuple tup = reader.next(); 
            int candidate = this.hashTupleH2(tup)%(this.numBuffers-1); 
            if (!(slots[candidate].contains(tup))) {
                slots[candidate].add(tup);
            } 
        }
        
        reader.close(); 

        outbatch = new Batch(batchsize);

        for (int i=0; i<this.numBuffers-1; i++) {
            for (int k = 0; k < slots[i].size(); k++) {
                outbatch.add(slots[i].get(k));
            }
        }

        partitionPointer++; 
        slots = null; 
        
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {

        this.cleanupTmpFiles(this.numBuffers - 1);

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
            String fileName = this.getTmpFileName(i);
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
                int candidateBucket = hashTupleH1(tup)%numOutputBuckets;
                outputPartitions[candidateBucket].next(tup);
            }
            this.inbatch = base.next(); 
        }

        for (int i=0; i<numOutputBuckets; i++) {
            outputPartitions[i].close();
        }
    }

    public int hashTupleH1(Tuple tup) {
        int sum = 0; 
        for (Object item : tup.data()) {
            sum += item.toString().hashCode();
        }
        return sum;
    }

    public int hashTupleH2(Tuple tup) {
        int sum = 0; 
        int multiplier = 1; 
        for (Object item : tup.data()) {
            sum += item.toString().hashCode()*multiplier;
            multiplier *= 2; 
        }
        return sum;
    }

    private String getTmpFileName(int i) {
        return this.hashCode() + "-Partition-" + this.uid + "-" + i + ".tmp";
    }

    private void cleanupTmpFiles(int numFiles) {
        for (int i = 0; i < numFiles; i++) {
            File tmpFile = new File(this.getTmpFileName(i)); 
            if (!tmpFile.delete()) {
                System.out.println("Could not delete file!");
            }
        }
    }
}
