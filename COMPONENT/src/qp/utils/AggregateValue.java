package qp.utils;

public class AggregateValue {
    int type; 
    int val; 
    int count = 0; // used for avg only
    int sum = 0; // used for avg only

    public AggregateValue(int type) {
        this.type = type; 
        if (type == Attribute.MAX) { this.val = Integer.MIN_VALUE; }
        else if (type == Attribute.MIN) { this.val = Integer.MAX_VALUE; }
        else if (type == Attribute.COUNT) { this.val = 0; }
        else if (type == Attribute.SUM) { this.val = 0; }
    }

    public void record(int val) {
        if (this.type == Attribute.MIN) {
            this.val = Math.min((int) this.val, val);
        } else if (this.type == Attribute.MAX) {
            this.val = Math.max((int) this.val, val);
        } else if (this.type == Attribute.COUNT) {
            this.val = (int) this.val + 1;
        } else if (this.type == Attribute.SUM) {
            this.val = (int) this.val + val;
        } else if (this.type == Attribute.AVG) {
            this.count = this.count + 1; 
            this.sum = this.sum + val; 
        }
    }

    public int get() {
        if (!(this.type == Attribute.AVG)) {
            return this.val;
        } else {
            return (this.sum / this.count);
        }
    }

}
