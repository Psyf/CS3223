package qp.utils;

public class TuplePair {
    Tuple leftTuple;
    Tuple rightTuple;

    public TuplePair(Tuple leftTuple, Tuple rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
    }

    public Tuple getLeftTuple() {
        return leftTuple;
    }

    public Tuple getRightTuple() {
        return rightTuple;
    }

    public Tuple joinTuple() {
        return leftTuple.joinWith(rightTuple);
    }
}
