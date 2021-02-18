package qp.utils;

public class AggregateValue {
    int type; 
    Object val; 
    int count = 0; // used for avg only
    int sum = 0; // used for avg only

    public AggregateValue(int type) {
        this.type = type; 
    }

    public void record(int val) {
        if (this.type == Attribute.MIN) {
            if (this.val == null) {
                this.val = val;
            } else {
                this.val = Math.min((int) this.val, val);
            }
        } else if (this.type == Attribute.MAX) {
            if (this.val == null) {
                this.val = val;
            } else {
                this.val = Math.max((int) this.val, val);
            }
        } else if (this.type == Attribute.COUNT) {
            if (this.val == null) {
                this.val = 1;
            } else {
                this.val = (int) this.val + 1;
            }
        } else if (this.type == Attribute.SUM) {
            if (this.val == null) {
                this.val = val;
            } else {
                this.val = (int) this.val + val;
            }
        } else if (this.type == Attribute.AVG) {
            this.count = this.count + 1; 
            this.sum = this.sum + val; 
        }
    }

    public int get() {
        if (!(this.type == Attribute.AVG)) {
            return (int) this.val;
        } else {
            return (this.sum / this.count);
        }
    }

}
