package qp.utils;

public class LogFunction {
    
    // Default constructor
    public LogFunction() {

    };

    public double calculate(double value, long base) {
        double answer = Math.log10(value) / Math.log10(base);
        return Math.ceil(answer);
    }
}
