package qp.operators;

import java.io.File;
import java.util.ArrayList;

import qp.utils.Batch;
import qp.utils.BatchList;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

public class ExternalSort extends Operator {

    Operator base;
    ArrayList<Integer> sortIndices;
    int numBuffers;
    String prefix;

    int batchsize;                  // Number of tuples per out batch
    Batch outbatch;                 // Buffer page for output
    Batch inBatch;                  // Buffer page for input
    int tuplesize;                  // Size of tuple
    TupleReader finalReader;        // Represents the final reader for the sorted file

    BatchList batchlist;            // Represents num of tuples in buffer
    int diskPointer = 0;            // Represents pointer for the base

    TupleReader[] tupleReaders;     // Represents the tuple reader during the merging stage
    BatchList inputBuffers;         // Represents the input buffers during the merging stage
    int passNo;                     // Represents the current number of passes
    ArrayList<Integer> previousRunNo; // Represents the list of run number, for purpose of cleanup

    public ExternalSort(String prefix, Operator base, ArrayList<Integer> sortIndices, int numBuffers) {
        super(OpType.EXTERNAL_SORT);
        this.prefix = prefix;
        this.base = base;
        this.sortIndices = sortIndices;
        this.numBuffers = numBuffers;
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        /** select number of tuples per batch **/
        tuplesize = base.getSchema().getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize; 

        try {
            // Phase 1: Generate sorted runs
            int numSortedRuns = generateSortedRuns(this.base, this.numBuffers, this.sortIndices, batchsize);

            // System.out.println("File 0-0:" + prefix);
            // Debug.PPrint(getSortedRunsFileName(0, 0), batchsize);
            // System.out.println("File 0-1:"  + prefix);
            // Debug.PPrint(getSortedRunsFileName(0, 1), batchsize);

            System.out.println("Start Phase 2");

            // Phase 2: Merge Sorted Runs
            initialiseMergeSortedRuns(numSortedRuns);

            // Open a reader to the sorted file
            finalReader = new TupleReader(getSortedRunsFileName(passNo, 0), batchsize);
            finalReader.open(); 

            System.out.println(prefix + "Compeleted file: ");
            Debug.PPrint(getSortedRunsFileName(passNo, 0), batchsize);
        } catch (Exception ex) {
            System.out.println("Problem with external sort");
            ex.printStackTrace();
            return false;
        }
        
        return true;
    }


    public int generateSortedRuns(Operator base, int numBuffers, ArrayList<Integer> sortIndices, int batchsize) {

        /** initialise the batchlist used for generating sorted runs **/
        // 1 buffer reserved for output
        batchlist = new BatchList(tuplesize, numBuffers); 

        int numRuns = 0; 

        Batch nextBatch = base.next(); 
    
        while (nextBatch != null) {

            while(!batchlist.isFull()) {
                // read batches into the batchlist
                batchlist.addBatch(nextBatch);
                nextBatch = base.next(); 
                if (nextBatch == null) {
                    break;
                }
            }

            // sort
            batchlist.sort(sortIndices); 
            
            // write to output 
            // DISCLAIMER: USING ADDITIONAL BUFFER
            String sortedRunFileName = getSortedRunsFileName(0, numRuns); 
            TupleWriter writer = new TupleWriter(sortedRunFileName, batchsize);
            writer.open();
            for (int i = 0 ; i < batchlist.size(); i++) {
                writer.next(batchlist.get(i));
            }
            writer.close();
            
            // // DEBUG: PLEASE COMMENT OUT
            // System.out.printf(">>> SORTED RUN BELOW %d \n", numRuns+1); 
            // Debug.PPrint(batchlist);

            batchlist.clear();

            if (nextBatch != null) { nextBatch = base.next(); }
            numRuns++; 
        }

        return numRuns; 
    }

    public void initialiseMergeSortedRuns(int numSortedRuns) {
        int inputBufferSize = numBuffers - 1;
        passNo = 1;

        System.out.println("NumofSortedRuns:" + numSortedRuns);
        System.out.println("Inputbuffersize:" + inputBufferSize);

        inputBuffers = new BatchList(tuplesize, inputBufferSize);

        tupleReaders = new TupleReader[numSortedRuns];
        previousRunNo = new ArrayList<>();

        mergeSortedRuns(numSortedRuns);
    }
    
    public void mergeSortedRuns(int numSortedRuns) {
        int inputBufferSize = numBuffers - 1;
        int runNo = 0;

        int start = 0;
        int end = inputBufferSize;
        boolean continueRun = true;

        previousRunNo.add(numSortedRuns);

        // Keep creating sorted runs within one pass
        while (continueRun) {

            // Create a tuple reader for the previous sorted runs 
            for (int i = start; i < end; i++) {
                if (i == numSortedRuns) {
                    end = i;
                    break;
                }
                TupleReader reader = new TupleReader(getSortedRunsFileName(passNo - 1, i), batchsize);
                reader.open();
                tupleReaders[i] = reader;
                System.out.println("Reader for file: " + getSortedRunsFileName(passNo - 1, i));
            }
            
            // Variables for writing to base
            String sortedFileName = getSortedRunsFileName(passNo, runNo);
            TupleWriter currentSortedRun = new TupleWriter(sortedFileName, batchsize);
            currentSortedRun.open();

            // Keep merging until the reader is empty
            boolean continueMerge = true;
            while (continueMerge) {
                continueMerge = mergeSortedRunsBetween(start, end, tupleReaders, 
                    inputBuffers, passNo, runNo, currentSortedRun);
            }

            currentSortedRun.close();
            
            runNo++;
            start += inputBufferSize;
            
            // If it is the last run within the pass, end should be equal to num of sorted runs
            if (end == numSortedRuns) {
                // Last run
                continueRun = false;
            } else {
                end += inputBufferSize;
            }
        }
        System.out.println("PassNo: " + passNo + " RunNo:" + runNo);
        // Debug.PPrint(getSortedRunsFileName(passNo-1, runNo), batchsize);
        // Last pass / Base case
        if (runNo == 1) {
            return ;
        }
        passNo++;
        
        mergeSortedRuns(runNo);
    }

    public boolean mergeSortedRunsBetween(int start, int end, TupleReader[] tupleReaders, 
        BatchList inputBuffers, int passNo, int runNo, TupleWriter currentSortedRun) {

        Batch outBatch = new Batch(batchsize);

        System.out.println(prefix + " mergeSortedRunsBetween:" + start + " To " + end);

        boolean endOfReader = false;

        int numOfEOFFile = 0;

        // Only work on reading from the start to the current end 
        outerloop:
        while (!inputBuffers.isFull()) {
            numOfEOFFile = 0;
            for (int i = start; i < end; i++) {
                // System.out.println("i: " + i);
                // Check if there is any more values left in all the reader
                if (tupleReaders[i].isEOF()) {
                    numOfEOFFile++; 
                    endOfReader = true;
                } else {
                    Tuple value = tupleReaders[i].next();
                    if (value != null) {
                        inputBuffers.addTuple(value);
                    }
                }
                if (numOfEOFFile == (end - start)) {
                    System.out.println("Nothing left in all readers." + tupleReaders[i].getFileName());
                    for (TupleReader reader: tupleReaders) {
                        if (reader != null) {
                            reader.close();
                        }
                    }
                    break outerloop;
                }
            }
        }

        if (inputBuffers == null || inputBuffers.isEmpty()) {
            System.out.println("Empty input buffers");
            return false;
        }
        // Debug.PPrint(inputBuffers);
        //Sort the inputBuffers tuples, so that we can get the smallest value out first
        inputBuffers.sort(sortIndices);
        
        // When the input buffer is empty, the base is empty
        // Add the smallest value to the outBatch repeatedly until out batch is full
        while (!outBatch.isFull() && !inputBuffers.isEmpty()) {
            Tuple tuple = inputBuffers.get(0);
            outBatch.add(tuple);
            inputBuffers.remove(0);
        }
        
        // Write all the values in out batch to the current sorted run
        for (int outBatchPointer = 0; outBatchPointer < outBatch.size(); outBatchPointer++) {
            currentSortedRun.next(outBatch.get(outBatchPointer));
        }
        outBatch.clear();
        if (endOfReader && inputBuffers.isEmpty()) {
            return false;
        }
        return true;
    }

    public Batch next() {
        Batch outBatch = new Batch(batchsize);
        // System.out.println("PassNo:" + passNo);

        if (finalReader.isEOF()) {
            finalReader.close();
            return null;
        }

        // Add tuples from the reader until the out batch is full and return it
        while (!outBatch.isFull()) {
            outBatch.add(finalReader.next());
        }
        
        return outBatch;
    }

    private String getSortedRunsFileName(int passNo, int runNo) {
        return prefix + "-ExternalSort-Pass-" + passNo + "-Run-" + runNo + ".tmp";
    }

    public boolean close() {
        // Clean up files 
        //TODO Change back
        // for (int i = 0; i <= passNo; i++) {
        //     for (int j = 0; j < previousRunNo.size(); j++) {
        //         for (int runNo = 0; runNo < previousRunNo.get(j); runNo++) {
        //             File tmpFile = new File(getSortedRunsFileName(i, runNo));
        //             if (!tmpFile.delete()) {
        //                 System.out.println("Error deleting :"  + tmpFile);
        //             }
        //             System.out.println("Delete: " + tmpFile);
        //         }
        //     }
        // }
        
        // Clean up variables
        batchlist.clear();
        inputBuffers.clear();
        inBatch = null;
        outbatch = null;
        
        return true;
    }
    
}
