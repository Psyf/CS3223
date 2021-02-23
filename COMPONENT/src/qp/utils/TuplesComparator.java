package qp.utils;
import java.util.ArrayList;
import java.util.Comparator;

public class TuplesComparator implements Comparator<Tuple> {
    ArrayList<Integer> sortIndices;

    public TuplesComparator(ArrayList<Integer> sortIndices) {
        this.sortIndices = sortIndices;
    }

    public int compare(Tuple t1, Tuple t2) {
        return Tuple.compareTuples(t1, t2, sortIndices, sortIndices);
    }
}
