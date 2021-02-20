package qp.utils;

import java.util.ArrayList;

public class Block {

    ArrayList<Tuple> block;
    static int BlockSize;
    static int numBatchesPerBlock;

    

    public Block(int tuplesize, int numBuff) {
        numBatchesPerBlock = Math.max(1, numBuff - 2);
        BlockSize = Batch.getPageSize() / tuplesize * numBatchesPerBlock;
        block = new ArrayList<Tuple>(numBatchesPerBlock);
    }

    /** Returns size of block in bytes */
    public int getBlockSize() {
        return BlockSize;
    }

    /** Insert the record in the block at the next free location */
    public void add(Tuple t) {
        block.add(t);
    }

    /** Clears the block / buffer */
    public void clear() {
        block.clear();
    }

    public boolean contains(Tuple t) {
        return block.contains(t);
    }

    public Tuple get(int i) {
        return block.get(i);
    }

    public int indexOf(Tuple t) {
        return block.indexOf(t);
    }

    public void add(Tuple t, int i) {
        block.add(i, t);
    }

    public boolean isEmpty() {
        return block.isEmpty();
    }

    public void remove(int i) {
        block.remove(i);
    }

    public void set(Tuple t, int i) {
        block.set(i, t);
    }

    /** Returns current size of block */
    public int size() {
        return block.size();
    }
}
