package qp.operators;

import java.io.File;
import java.util.ArrayList;

import qp.utils.Batch;
import qp.utils.BatchList;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

public class ExternalSort extends Operator {

    Operator disk;
    ArrayList<Integer> diskIndexes;
    int numOfBuffer;
    String prefix;

    int batchsize;                  // Number of tuples per out batch
    Batch outbatch;                 // Buffer page for output
    Batch inBatch;                  // Buffer page for input
    int tuplesize;                  // Size of tuple
    TupleReader finalReader;        // Represents the final reader for the sorted file

    BatchList batchlist;            // Represents num of tuples in buffer
    int diskPointer = 0;            // Represents pointer for the disk

    TupleReader[] tupleReaders;     // Represents the tuple reader during the merging stage
    BatchList inputBuffers;         // Represents the input buffers during the merging stage
    int passNo;                     // Represents the current number of passes
    ArrayList<Integer> previousRunNo; // Represents the list of run number, for purpose of cleanup

    public ExternalSort(String prefix, Operator disk, ArrayList<Integer> diskIndexes, int numOfBuffer) {
        super(OpType.EXTERNAL_SORT);
        this.prefix = prefix;
        this.disk = disk;
        this.diskIndexes = diskIndexes;
        this.numOfBuffer = numOfBuffer;
    }

    @Override
    public boolean open() {
        if (!disk.open()) {
            return false;
        }
        /** select number of tuples per batch **/
        tuplesize = disk.getSchema().getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** initialise the batchlist used for the join */
        batchlist = new BatchList(tuplesize, numOfBuffer);

        try {
            // Phase 1: Generate sorted runs
            int numOfSortedRuns = generateSortedRuns();

            System.out.println("Start Phase 2");

            // Phase 2: Merge Sorted Runs
            initialiseMergeSortedRuns(numOfSortedRuns);

            // Open a reader to the sorted file
            finalReader = new TupleReader(getSortedRunsFileName(passNo, 0), batchsize);
            finalReader.open(); 
        } catch (Exception ex) {
            System.out.println("Problem with external sort");
            ex.printStackTrace();
            return false;
        }
        
        return true;
    }

    public int generateSortedRuns() {
        int numOfSortedRuns = 0;
        inBatch = new Batch(batchsize);
        inBatch = disk.next();

        diskPointer = 0;
        
        // While the disk is not empty, keep on generating sorted run
        while (inBatch != null) {
            generateSortedRun(numOfSortedRuns);
            numOfSortedRuns++;
        }

        return numOfSortedRuns;
    }

    public void generateSortedRun(int sortedRunNum) {

        System.out.println("Before: " + batchlist.isFull());

        // Add in the values from disk to buffers until there is nothing left in the disk or 
        // the batch list is full.
        while (!batchlist.isFull()) {
            // Reset diskPointer to read from the first value in the next inbatch
            diskPointer = 0;
            
            if (inBatch == null) break;
            while (diskPointer < inBatch.size()) {
                Tuple nextTuple = inBatch.get(diskPointer); 
                batchlist.addTuple(nextTuple);
                diskPointer++;
            }
            
            inBatch = disk.next();
            System.out.println("Inner loop runs:" + inBatch);
        }

        //Sort the batchlist 
        batchlist.sort(diskIndexes);

        // Initialise variables to write sorted runs
        String sortedRunFileName = getSortedRunsFileName(0, sortedRunNum); 
        TupleWriter currentSortedRun = new TupleWriter(sortedRunFileName, batchsize);
        int batchListPointer = 0;
        currentSortedRun.open();

        System.out.println("After: " + batchlist.isFull());
        
        // Write the sorted run into disk 
        while (batchListPointer < batchlist.size()) {
            currentSortedRun.next(batchlist.get(batchListPointer));
            batchListPointer++;
        }
        try {
            batchlist.clear();
            currentSortedRun.close();
        } catch (Exception ex) {
            System.out.println("Error here");
        }
        return;
    }

    public void initialiseMergeSortedRuns(int numOfSortedRuns) {
        int inputBufferSize = numOfBuffer - 1;
        passNo = 1;

        System.out.println("NumofSortedRuns:" + numOfSortedRuns);
        System.out.println("Inputbuffersize:" + inputBufferSize);

        inputBuffers = new BatchList(tuplesize, inputBufferSize);

        tupleReaders = new TupleReader[numOfSortedRuns];
        previousRunNo = new ArrayList<>();

        mergeSortedRuns(numOfSortedRuns);
    }
    
    public void mergeSortedRuns(int numOfSortedRuns) {
        int inputBufferSize = numOfBuffer - 1;
        int runNo = 0;

        int start = 0;
        int end = inputBufferSize;
        boolean continueRun = true;

        previousRunNo.add(numOfSortedRuns);

        System.out.println("Pass no:" + start);
        System.out.println("Pass no:" + end);

        // Create a tuple reader for the previous sorted runs 
        for (int i = start; i < end; i++) {
            if (i == numOfSortedRuns) {
                end = i;
                break;
            }
            TupleReader reader = new TupleReader(getSortedRunsFileName(passNo - 1, i), batchsize);
            reader.open();
            tupleReaders[i] = reader;
        }
        

        // Keep creating sorted runs within one pass
        while (continueRun) {
            mergeSortedRunsBetween(start, end, tupleReaders, inputBuffers, passNo, runNo);
            
            runNo++;
            start += inputBufferSize;
            
            // If it is the last run within the pass, end should be equal to num of sorted runs
            if (end == numOfSortedRuns) {
                // Last run
                end = numOfSortedRuns;
                continueRun = false;
            } else {
                end += inputBufferSize;
            }
        }
        // Last pass / Base case
        if (runNo == 1) {
            return ;
        }
        passNo++;
        
        mergeSortedRuns(runNo);
    }

    public void mergeSortedRunsBetween(int start, int end, 
        TupleReader[] tupleReaders, BatchList inputBuffers, int passNo, int runNo) {

        Batch outBatch = new Batch(batchsize);

        System.out.println("mergeSortedRunsBetween:" + start);

        // Only work on reading from the start to the current end 
        for (int i = start; i < end; i++) {
            TupleReader reader = tupleReaders[i];
            inputBuffers.addTuple(reader.next());
        }

        //Sort the inputBuffers tuples, so that we can get the smallest value out first
        inputBuffers.sort(diskIndexes);
        
        // Add the smallest value to the outBatch repeatedly until out batch is full
        while (!outBatch.isFull() && !inputBuffers.isEmpty()) {
            Tuple tuple = inputBuffers.get(0);
            outBatch.add(tuple);
            inputBuffers.remove(0);
        }

        System.out.println("Size:" + outBatch.size());

        // Write to disk
        String sortedFileName = getSortedRunsFileName(passNo, runNo);
        TupleWriter currentSortedRun = new TupleWriter(sortedFileName, batchsize);
        currentSortedRun.open();

        for (int outBatchPointer = 0; outBatchPointer < outBatch.size(); outBatchPointer++) {
            System.out.println("PassNo writing to disk:" + passNo);
            currentSortedRun.next(outBatch.get(outBatchPointer));
        }
        currentSortedRun.close();
        outBatch.clear();
    }

    public Batch next() {
        Batch outBatch = new Batch(batchsize);

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
        return prefix + "-SortMergeBatch-P-" + passNo + "-R-" + runNo;
    }

    public boolean close() {
        // Clean up files 
        for (int i = 0; i <= passNo; i++) {
            for (int j = 0; j < previousRunNo.size(); j++) {
                for (int runNo = 0; runNo < previousRunNo.get(j); runNo++) {
                    File tmpFile = new File(getSortedRunsFileName(i, runNo));
                    tmpFile.delete();
                }
            }
        }
        
        // Clean up variables
        batchlist.clear();
        inputBuffers.clear();
        inBatch = null;
        outbatch = null;
        
        return true;
    }
    
}
