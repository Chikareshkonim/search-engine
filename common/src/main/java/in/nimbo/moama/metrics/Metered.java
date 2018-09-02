package in.nimbo.moama.metrics;

public interface Metered {
    String stat(float delta);
}