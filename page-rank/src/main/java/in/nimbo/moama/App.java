package in.nimbo.moama;

import in.nimbo.moama.rankcalculator.ReferenceCalculator;

public class App {
    public static void main(String[] args) {
        ReferenceCalculator referenceCalculator;
        referenceCalculator = new ReferenceCalculator("refrence","s2");
        referenceCalculator.calculate();
    }
}
