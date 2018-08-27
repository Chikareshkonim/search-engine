package in.nimbo.moama;

import static java.lang.Thread.MAX_PRIORITY;

public class Config {
    public static final int NUMBER_OF_FETCHER_THREADS = 20;
    public static final int NUMBER_OF_READER_THREADS = 20;
    public static final int FETCHER_THREAD_PRIORITY = MAX_PRIORITY - 2;
    public static final int READER_THREAD_PRIORITY = MAX_PRIORITY - 2;
}
