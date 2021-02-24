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
    int tuplesize;                  // Size of tuple

    int lastPassIndex; 
    int direction;                  // 0 is for ASC, 1 is for DESC

    Batch outBatch;
    TupleReader inBatch; 

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
            int numSortedRuns = generateSortedRuns(this.base, this.numBuffers, this.sortIndices, this.batchsize, this.direction);
            // System.out.printf("> Phase 1: Num Sorted Runs: %d\n", numSortedRuns); 

            // System.out.println("File 0-0:" + prefix);
            // Debug.PPrint(getSortedRunsFileName(0, 0), batchsize);
            // System.out.println("File 0-1:"  + prefix);
            // Debug.PPrint(getSortedRunsFileName(0, 1), batchsize);

            // Phase 2: Merge Sorted Runs
            this.lastPassIndex = mergeSortedRuns(0, numSortedRuns, this.numBuffers, this.batchsize, direction);

            // Open a reader to the sorted file
            // TupleReader finalReader;        // Represents the final reader for the sorted file
            // finalReader = new TupleReader(getSortedRunsFileName(this.lastPassIndex, 0), batchsize);
            // finalReader.open(); 
            // while (!finalReader.isEOF()) {
            //     Debug.PPrint(finalReader.next());
            // }
            // finalReader.close();
        } catch (Exception ex) {
            System.out.println("Problem with external sort");
            ex.printStackTrace();
            return false;
        }
        
        inBatch = new TupleReader(getSortedRunsFileName(this.lastPassIndex, 0), this.batchsize);
        inBatch.open(); 

        return true;
    }


    public int generateSortedRuns(Operator base, int numBuffers, ArrayList<Integer> sortIndices, int batchsize, int direction) {

        /** initialise the batchlist used for generating sorted runs **/
        // 1 buffer reserved for output
        BatchList batchlist = new BatchList(tuplesize, numBuffers); 

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

    public Batch next() {
        if (inBatch.isEOF()) {
            return null; 
        }
        outBatch.clear(); 
        while (!outBatch.isFull()) {
            Tuple nextTuple = inBatch.next(); 
            if (nextTuple == null) { break; }
            else { outBatch.add(nextTuple); }
        }
        return outBatch; 
    }

    // mode = 0 = min
    // mode = 1 = max
    // Return: 0 if t1 is result, 1 if t2 is result
    public int compareTuples(Tuple t1, Tuple t2, ArrayList<Integer> sortIndices, int mode) {
        int compareValue = Tuple.compareTuples(t1,t2, sortIndices, sortIndices);
        if (mode == 0) { // MIN MODE
            if (compareValue < 0) { return 0; }
            else { return 1; }
        } else if (mode == 1) { // MAX MODE
            if (compareValue >= 0) { return 0; }
            else { return 1; }
        } else {
            System.out.println("Please use either min or max!");
            System.exit(1);
            return 0; // Thanks Java, you useless PoS
        }
    }

    public int mergeSortedRuns(int passNum, int numSortedRuns, int numBuffers, int batchsize, int direction) {

        int numOutputRuns = 0;
        int unreadFileIndex = 0; 
        int numInputBuffers = numBuffers - 1;

        // System.out.printf(">> Starting with Pass=%d\n", passNum); 

        // This WHILE loop is for the entire pass
        // Each execution of the while loop takes in k files for a k way merge sort
        // Do this until all files of this pass are done!
        while (unreadFileIndex < numSortedRuns) {

            // System.out.printf(">>> Starting with File=%d\n", unreadFileIndex); 
            TupleReader[] inputBuffers = new TupleReader[numInputBuffers];
            int numOpenFiles = 0; 

            for (int i = 0; i < numInputBuffers; i++) {
                String inputFname = getSortedRunsFileName(passNum, i + unreadFileIndex);
                inputBuffers[i] = new TupleReader(inputFname, batchsize);
                if (inputBuffers[i].open()) { numOpenFiles++; }
            }

            String outputFname = getSortedRunsFileName(passNum+1, numOutputRuns);
            TupleWriter outputBuffer = new TupleWriter(outputFname, batchsize); 
            outputBuffer.open(); 
            boolean tupleRemaining = true; 

            while (tupleRemaining) {
                // k-way merge
                tupleRemaining = false; 
                Tuple candidateTuple = null;
                int candidateBuffer = 0; 
                for (int i = 0; i < numOpenFiles; i++) {
                    if (!inputBuffers[i].isEOF()) {
                        Tuple nextTuple = inputBuffers[i].peek(); 
                        if (candidateTuple == null) { 
                            candidateTuple = nextTuple; 
                            candidateBuffer = i;
                            tupleRemaining = true; 
                        } 
                        else {
                            if (compareTuples(candidateTuple, nextTuple, sortIndices, direction) == 1) { 
                                candidateTuple = nextTuple; 
                                candidateBuffer = i; 
                                tupleRemaining = true; 
                            } 
                        }
                    }
                }
                if (candidateTuple != null) {
                    inputBuffers[candidateBuffer].next(); // consume
                    outputBuffer.next(candidateTuple);
                }
            }
            outputBuffer.close(); 

            // close all TupleReaders
            for (int i = 0; i < numOpenFiles; i++) { inputBuffers[i].close(); }
            numOutputRuns++;     
            unreadFileIndex += numOpenFiles; 
        }

        // System.out.printf("<< NumOutputRuns=%d\n", numOutputRuns); 

        if (numOutputRuns > 1) { return 1 + mergeSortedRuns(passNum+1, numOutputRuns, numBuffers, batchsize, direction); }
        return 1; 
    }

    private String getSortedRunsFileName(int passNo, int runNo) {
        return prefix + "-ExternalSort-Pass-" + passNo + "-Run-" + runNo + ".tmp";
    }

    public boolean close() {
        // Clean up files 
        cleanupTmpFiles(this.lastPassIndex);
        inBatch.close();
        outBatch.clear(); 
        return true;
    }

    private void cleanupTmpFiles(int totalNumPasses) {
        for (int i = 0; i <= totalNumPasses; i++) {
            int j = 0;
            while (true) {
                System.out.printf("Deleting file: %s\n", this.getSortedRunsFileName(i, j));
                File tmpFile = new File(this.getSortedRunsFileName(i, j)); 
                if (!tmpFile.delete()) { break; }
                j++; 
            }
        }
    }
}
