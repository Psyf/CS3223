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

    int batchsize;                  // Number of tuples per out batch
    Batch outbatch;                 // Buffer page for output
    Batch inBatch;                  // Buffer page for input
    int tuplesize;                  // Size of tuple

    BatchList batchlist;            // Represents num of tuples in buffer

    TupleReader[] tupleReaders;     // Represents the tuple reader during the merging stage
    BatchList inputBuffers;         // Represents the input buffers during the merging stage
    int passNo;                     // Represents the current number of passes
    ArrayList<Integer> previousRunNo; // Represents the list of run number, for purpose of cleanup

    public ExternalSort(Operator disk, ArrayList<Integer> diskIndexes, int numOfBuffer) {
        super(OpType.EXTERNAL_SORT);
        this.disk = disk;
        this.diskIndexes = diskIndexes;
        this.numOfBuffer = numOfBuffer;
    }

    @Override
    public boolean open() {
        if (disk.open()) {
            return true;
        }
        /** select number of tuples per batch **/
        tuplesize = disk.getSchema().getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** initialise the batchlist used for the join */
        batchlist = new BatchList(tuplesize, numOfBuffer);

        try {
            // Phase 1: Generate sorted runs
            int numOfSortedRuns = generateSortedRuns();

            // Phase 2: Merge Sorted Runs
            mergeSortedRuns(numOfSortedRuns);
        } catch (Exception ex) {
            System.out.println("Problem with external sort");
            return false;
        }
        
        return true;
    }

    public int generateSortedRuns() {
        int numOfSortedRuns = 0;
        inBatch = new Batch(batchsize);
        inBatch = disk.next();
        
        // While the disk is not empty, keep on generating sorted run
        while (!inBatch.isEmpty()) {
            generateSortedRun(numOfSortedRuns);
            numOfSortedRuns++;
        }

        return numOfSortedRuns;
    }

    public void generateSortedRun(int sortedRunNum) {
        
        int diskPointer = 0;

        // Add in the values from disk to buffers until there is nothing left in the disk or 
        // the batch list is full.
        while (!batchlist.isFull() && inBatch != null) {
            while (diskPointer < inBatch.size()) {
                Tuple nextTuple = inBatch.get(diskPointer); 
                batchlist.addTuple(nextTuple);
                diskPointer++;
            }
            // Reset diskPointer to read from the first value in the next inbatch
            diskPointer = 0;
            inBatch = disk.next();
        }

        //Sort the batchlist 
        batchlist.sort(diskIndexes);

        // Initialise variables to write sorted runs
        String sortedRunFileName = getSortedRunsFileName(0, sortedRunNum); 
        TupleWriter currentSortedRun = new TupleWriter(sortedRunFileName, batchsize);
        int batchListPointer = 0;
        currentSortedRun.open();
        
        // Write the sorted run into disk 
        while (batchListPointer < batchlist.size()) {
            currentSortedRun.next(batchlist.get(batchListPointer));
            batchListPointer++;
        }
        currentSortedRun.close();
    }

    public void initialiseMergeSortedRuns(int numOfSortedRuns) {
        int inputBufferSize = numOfBuffer - 1;
        passNo = 1;

        inputBuffers = new BatchList(tuplesize, inputBufferSize);

        tupleReaders = new TupleReader[numOfSortedRuns];
        previousRunNo = new ArrayList<>();

        mergeSortedRuns(numOfSortedRuns);
    }
    
    public void mergeSortedRuns(int numOfSortedRuns) {
        int inputBufferSize = numOfBuffer - 1;
        int runNo = 1;

        int start = 0;
        int end = inputBufferSize;
        boolean lastRun = false;

        previousRunNo.add(numOfSortedRuns);

        // Create a tuple reader for the previous sorted runs 
        for (int i = 0; i < inputBufferSize; i++) {
            TupleReader reader = new TupleReader(getSortedRunsFileName(passNo - 1, i), batchsize);
            reader.open();
            tupleReaders[i] = reader;
        }

        // Keep creating sorted runs within one pass
        while (true) {
            mergeSortedRunsBetween(start, end, tupleReaders, inputBuffers, passNo, runNo);
            if (lastRun) break;
            
            runNo++;
            start += inputBufferSize;
            
            // If it is the last run within the pass, end should be equal to num of sorted runs
            if (end + inputBufferSize < numOfSortedRuns) {
                end += inputBufferSize;
            } else {
                // Last run
                end = numOfSortedRuns;
                lastRun = true;
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

        // Only work on reading from the start to the current end 
        for (int i = start; i < end; i++) {
            TupleReader reader = tupleReaders[i];
            inputBuffers.addTuple(reader.next());
        }

        //Sort the inputBuffers tuples, so that we can get the smallest value out first
        inputBuffers.sort(diskIndexes);
        
        // Add the smallest value to the outBatch repeatedly until out batch is full
        while (!outBatch.isFull()) {
            outBatch.add(inputBuffers.get(0));
            batchlist.remove(0);
        }

        // Write to disk
        for (int outBatchPointer = 0; outBatchPointer < outBatch.size(); outBatchPointer++) {
            String mergeFileName = getSortedRunsFileName(passNo, runNo);
            TupleWriter currentSortedRun = new TupleWriter(mergeFileName, batchsize);
            currentSortedRun.next(outBatch.get(outBatchPointer));
            outBatchPointer++;
        }
        outBatch.clear();
    }

    public Batch next() {
        Batch outBatch = new Batch(batchsize);

        // Open a reader to the sorted file
        TupleReader reader = new TupleReader(getSortedRunsFileName(passNo, 1), batchsize);
        reader.open(); 

        // Add tuples from the reader until the out batch is full and return it
        while (!outBatch.isFull()) {
            outBatch.add(reader.next());
        }
        return outBatch;
    }

    private String getSortedRunsFileName(int passNo, int runNo) {
        return "SortMergeBatch-P:" + passNo + "R:" + runNo;
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
