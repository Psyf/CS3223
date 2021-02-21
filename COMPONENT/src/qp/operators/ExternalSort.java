package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;

import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TuplesComparator;

public class ExternalSort extends Operator {

    Operator disk;
    ArrayList<Integer> diskIndexes;
    int numOfBuffer;

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    Batch outbatch;                 // Buffer page for output
    Batch inBatch;                  // Buffer page for input
    String rfname;                  // The file name where the right table is materialized
    ObjectInputStream inStream;           // File pointer to the right hand materialized file
    ObjectOutputStream outStream;           // File pointer to the right hand materialized file

    public ExternalSort(Operator disk, ArrayList<Integer> diskIndexes, int numOfBuffer) {
        super(OpType.EXTERNAL_SORT);
        this.disk = disk;
        this.diskIndexes = diskIndexes;
        this.numOfBuffer = numOfBuffer;
    }

    @Override
    public boolean open() {
        //TODO
        if (disk.open()) {
            return true;
        }
        /** select number of tuples per batch **/
        int tuplesize = disk.getSchema().getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        // Phase 1: Generate sorted runs
        int numOfSortedRuns = generateSortedRuns();

        // Phase 2: Merge Sorted Runs
        mergeSortedRuns(numOfSortedRuns);

        // Writing to the disk itself
        rfname = "ExternalSortTemp-" + String.valueOf(0); //TODO
        try {
            inStream = new ObjectInputStream(new FileInputStream(rfname));
        } catch (Exception ex) {
            return false;
        }
        return true;
    }

    public int generateSortedRuns() {
        int numOfSortedRuns = 0;
        inBatch = new Batch(batchsize);

        while(inBatch != null) {
            addValuesToBuffer(numOfBuffer - 1);
            inBatch = disk.next();
            numOfSortedRuns++;
        }

        return numOfSortedRuns;
    }

    public void addValuesToBuffer(int avaliableBuffer) {
        
        ArrayList<Tuple> tupleList = new ArrayList<>();

        // Add values to the batch until full
        while (!inBatch.isFull()) {
            for (int i = 0; i < avaliableBuffer; i++) {
                if (inBatch.isEmpty()) {
                    break;
                }
                tupleList.add(outbatch.remove(0));
            }
            inBatch = disk.next();
        }

        Collections.sort(tupleList, new TuplesComparator(diskIndexes));
        
        try {
            ObjectOutputStream outStream = new ObjectOutputStream(new FileOutputStream(rfname));
            for (Tuple tuple : tupleList) {
                outStream.writeObject(tuple);
            }
            outStream.close();
        } catch (Exception ex) {
            System.exit(1);
        }
    }
    
    public void mergeSortedRuns(int numOfSortedRuns) {
        //TODO
        int inputBufferSize = numOfBuffer - 1;
        int newNumOfSortedRuns = 0;
        int tempNumOfSortedRuns = numOfSortedRuns; // Contains the current count of the num of sorted runs
        Batch inbatch = new Batch(batchsize); 

        try {
            inStream = new ObjectInputStream(new FileInputStream(rfname));
        } catch (Exception ex) {
            System.out.println("Reading file error from merge sorted runs");
            System.exit(1);
        }

        // Run passes on the sorted runs
        while (tempNumOfSortedRuns > 0) {
            for (int i = 0; i < inputBufferSize; i++) {
                tempNumOfSortedRuns--;
                try {
                    Tuple tuple = (Tuple) inStream.readObject();
                    inbatch.add(tuple);
                } catch (Exception ex) {
                    System.out.println("Problem reading from the inStream");
                    System.exit(1);
                }
                
            }
            newNumOfSortedRuns++;
        }

        // Last pass
        if (newNumOfSortedRuns == 1) {
            return;
        }
        mergeSortedRuns(newNumOfSortedRuns);
        
    }

    public Batch next() {
        Batch outBatch = new Batch(batchsize);

        while (!outBatch.isFull()) {
            try {
                Tuple tupleValue = (Tuple) inStream.readObject();
                outBatch.add(tupleValue);
            } catch (EOFException e) {
                try {
                    inStream.close();
                } catch (IOException io) {
                    System.out.println("Externsort: Error in reading temporary file");
                }
            } catch (ClassNotFoundException c) {
                System.out.println("Externsort: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("Externsort: Error in reading temporary file");
                System.exit(1);
            }
        }
        return outBatch;
    }

    public boolean close() {
        try {
            inStream.close();
        } catch(Exception ex) {
            return false;
        }
        return true;
    }
    
}
