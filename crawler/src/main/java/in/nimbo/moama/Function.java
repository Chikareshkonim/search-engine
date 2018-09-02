package in.nimbo.moama;

import in.nimbo.moama.crawler.domainvalidation.HashDuplicateChecker;
import in.nimbo.moama.listener.CLI;
import in.nimbo.moama.metrics.Metrics;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

public class Function {
    @CLI(help = "load duplicate hash map")
    public static void loadDuplicate(PrintStream out, Scanner scanner) {
        out.println("load duplicate called");
        try {
            HashDuplicateChecker.getInstance().loadHashTable();
        } catch (IOException e) {
            e.printStackTrace(out);
        }
    }



    @CLI(help = "save duplicate hash map")
    public static void saveDuplicate(PrintStream out, Scanner scanner) {
        out.println("save duplicate called");
        HashDuplicateChecker.getInstance().saveHashTable();
    }

    public static void stat(PrintStream out, Scanner scanner) {
        Metrics.stat(out::println);
    }

    @CLI(help = "exit program" )
    public static void exit(PrintStream out, Scanner scanner) {
        saveDuplicate(out, scanner);
        out.println("done");
        System.exit(0);
    }

}

