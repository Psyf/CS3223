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
    int passNo;                     // Represents the current number of passes
    ArrayList<Integer> previousRunNo; // Represents the list of run number, for purpose of cleanup

    int direction;                  // 0 is for ASC, 1 is for DESC

    public ExternalSort(String prefix, Operator base, ArrayList<Integer> sortIndices, int numBuffers, int direction) {
        super(OpType.EXTERNAL_SORT);
        this.prefix = prefix;
        this.base = base;
        this.sortIndices = sortIndices;
        this.numBuffers = numBuffers;
        this.direction = direction; 
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
            int numSortedRuns = generateSortedRuns(this.base, this.numBuffers, this.sortIndices, batchsize, this.direction);

            // System.out.println("File 0-0:" + prefix);
            // Debug.PPrint(getSortedRunsFileName(0, 0), batchsize);
            // System.out.println("File 0-1:"  + prefix);
            // Debug.PPrint(getSortedRunsFileName(0, 1), batchsize);

            // Phase 2: Merge Sorted Runs
            //mergeSortedRuns(numSortedRuns, this.numBuffers);


            
            
            
            
            // IGNORE HENCEFORTH 






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


    public int generateSortedRuns(Operator base, int numBuffers, ArrayList<Integer> sortIndices, int batchsize, int direction) {

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
            batchlist.sort(sortIndices, direction); 
            
            // write to output 
            // DISCLAIMER: USING ADDITIONAL BUFFER
            String sortedRunFileName = getSortedRunsFileName(0, numRuns); 
            TupleWriter writer = new TupleWriter(sortedRunFileName, batchsize);
            writer.open();
            for (int i = 0 ; i < batchlist.size(); i++) {
                writer.next(batchlist.get(i));
            }
            writer.close();
            
            // DEBUG: PLEASE COMMENT OUT
            // System.out.printf(">>> SORTED RUN BELOW %d \n", numRuns+1); 
            // Debug.PPrint(batchlist);

            batchlist.clear();

            if (nextBatch != null) { nextBatch = base.next(); }
            numRuns++; 
        }

        return numRuns; 
    }



//     // TODO: PLEASE DELETE
//     public void initialiseMergeSortedRuns(int numSortedRuns) {
//         int inputBufferSize = numBuffers - 1;
//         passNo = 1;

//         // System.out.println("NumofSortedRuns:" + numSortedRuns);
//         // System.out.println("Inputbuffersize:" + inputBufferSize);

//         //inputBuffers = new BatchList(tuplesize, inputBufferSize);

//         tupleReaders = new TupleReader[numSortedRuns];
//         previousRunNo = new ArrayList<>();

//         //mergeSortedRuns(numSortedRuns);
//     }
    
    // Tuples.compareTuples -> 
    // 0 if equal 
    // negative if t1 is smaller 
    // positive if t1 is bigger

    // mode = 0 = min
    // mode = 1 = max
    public Tuple compareTuples(Tuple t1, Tuple t2, ArrayList<Integer> sortIndices, int mode) {
        int compareValue = Tuple.compareTuples(t1,t2, sortIndices, sortIndices);
        if (mode == 0) { // MIN MODE
            if (compareValue < 0) { return t1; }
            else { return t2; }
        } else if (mode == 1) { // MAX MODE
            if (compareValue >= 0) { return t1; }
            else { return t2; }
        } else {
            System.out.println("Please use either min or max!");
            System.exit(1);
            return t1; // Thanks Java you useless PoS
        }
    }

    public void mergeSortedRuns(int numSortedRuns, int numBuffers, int batchsize, int direction) {

        int numOutputRuns = 0;

        int inputBufferSize = numBuffers - 1;

        TupleReader[] inputBuffers = new TupleReader[inputBufferSize];

        for (int i = 0; i < inputBufferSize; i++) {
            String inputFname = getSortedRunsFileName(0, i); // pass number will change
            inputBuffers[i] = new TupleReader(inputFname, batchsize);
            inputBuffers[i].open();
        }

        
        String outputFname = getSortedRunsFileName(1, numOutputRuns);
        TupleWriter outputBuffer = new TupleWriter(outputFname, batchsize); 


        // k-way merge
        Tuple candidateTuple;  
        for (int i = 0; i < inputBufferSize; i++) {
            // PICKUP FROM HERE: COMPARETUPLES WRITTEN
        }


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
        
        //mergeSortedRuns(runNo);
    }

//     public boolean mergeSortedRunsBetween(int start, int end, TupleReader[] tupleReaders, 
//         BatchList inputBuffers, int passNo, int runNo, TupleWriter currentSortedRun) {

//         Batch outBatch = new Batch(batchsize);

//         System.out.println(prefix + " mergeSortedRunsBetween:" + start + " To " + end);

//         boolean endOfReader = false;

//         int numOfEOFFile = 0;

//         // Only work on reading from the start to the current end 
//         outerloop:
//         while (!inputBuffers.isFull()) {
//             numOfEOFFile = 0;
//             for (int i = start; i < end; i++) {
//                 // System.out.println("i: " + i);
//                 // Check if there is any more values left in all the reader
//                 if (tupleReaders[i].isEOF()) {
//                     numOfEOFFile++; 
//                     endOfReader = true;
//                 } else {
//                     Tuple value = tupleReaders[i].next();
//                     if (value != null) {
//                         inputBuffers.addTuple(value);
//                     }
//                 }
//                 if (numOfEOFFile == (end - start)) {
//                     System.out.println("Nothing left in all readers." + tupleReaders[i].getFileName());
//                     for (TupleReader reader: tupleReaders) {
//                         if (reader != null) {
//                             reader.close();
//                         }
//                     }
//                     break outerloop;
//                 }
//             }
//         }

//         if (inputBuffers == null || inputBuffers.isEmpty()) {
//             System.out.println("Empty input buffers");
//             return false;
//         }
//         // Debug.PPrint(inputBuffers);
//         //Sort the inputBuffers tuples, so that we can get the smallest value out first
//         inputBuffers.sort(sortIndices, 0);
        
//         // When the input buffer is empty, the base is empty
//         // Add the smallest value to the outBatch repeatedly until out batch is full
//         while (!outBatch.isFull() && !inputBuffers.isEmpty()) {
//             Tuple tuple = inputBuffers.get(0);
//             outBatch.add(tuple);
//             inputBuffers.remove(0);
//         }
        
//         // Write all the values in out batch to the current sorted run
//         for (int outBatchPointer = 0; outBatchPointer < outBatch.size(); outBatchPointer++) {
//             currentSortedRun.next(outBatch.get(outBatchPointer));
//         }
//         outBatch.clear();
//         if (endOfReader && inputBuffers.isEmpty()) {
//             return false;
//         }
//         return true;
//     }

//     public Batch next() {
//         Batch outBatch = new Batch(batchsize);
//         // System.out.println("PassNo:" + passNo);

//         if (finalReader.isEOF()) {
//             finalReader.close();
//             return null;
//         }

//         // Add tuples from the reader until the out batch is full and return it
//         while (!outBatch.isFull()) {
//             outBatch.add(finalReader.next());
//         }
        
//         return outBatch;
//     }

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
        //inputBuffers.clear();
        inBatch = null;
        outbatch = null;
        
        return true;
    }
    
}
