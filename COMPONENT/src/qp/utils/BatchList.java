package qp.utils;

import java.util.ArrayList;
import java.util.Collections;

public class BatchList {

    ArrayList<Tuple> batchlist;
    static int BatchListSize;
    static int numBatches;

    public BatchList(int tuplesize, int numBuff) {
        numBatches = Math.max(1, numBuff);
        BatchListSize = Batch.getPageSize() / tuplesize * numBatches;
        batchlist = new ArrayList<Tuple>(numBatches);
    }

    /** Returns size of block in bytes */
    public int getMaxSize() {
        return BatchListSize;
    }

    /** Checks if current batchlist has exceeded max number of tuples allowed in a batchlist */
    public boolean isFull() {
        if (batchlist.size() <= BatchListSize) {
            return false;
        }
        return true;
    }

    /** Insert the record in the block at the next free location */
    public void addTuple(Tuple t) {
        batchlist.add(t);
    }

    /** Inserts all records in a batch to the block at the next free location */
    public void addBatch(Batch b) {
        for(int i = 0; i < b.size(); i++) {
            batchlist.add(b.get(i));
        }
    }

    /** Clears the block / buffer */
    public void clear() {
        batchlist.clear();
    }

    public boolean contains(Tuple t) {
        return batchlist.contains(t);
    }

    public Tuple get(int i) {
        return batchlist.get(i);
    }

    public int indexOf(Tuple t) {
        return batchlist.indexOf(t);
    }

    public void add(Tuple t, int i) {
        batchlist.add(i, t);
    }

    public boolean isEmpty() {
        return batchlist.isEmpty();
    }

    public void remove(int i) {
        batchlist.remove(i);
    }

    public void set(Tuple t, int i) {
        batchlist.set(i, t);
    }

    /** Returns current size of batchlist */
    public int size() {
        return batchlist.size();
    }

    public void sort(ArrayList<Integer> sortIndices, int direction) {
        Collections.sort(batchlist, new TuplesComparator(sortIndices));
        if (direction == 1) { // 1 == DESCENDING
            Collections.reverse(batchlist);
        } 
    }
}

