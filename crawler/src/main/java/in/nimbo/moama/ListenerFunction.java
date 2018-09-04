package in.nimbo.moama;

import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.crawler.domainvalidation.HashDuplicateChecker;
import in.nimbo.moama.listener.CLI;
import in.nimbo.moama.metrics.Metrics;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

public class ListenerFunction {
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
    @CLI(help = "show you statistic of this process")
    public static void stat(PrintStream out, Scanner scanner) {
        Metrics.stat(out::println);
    }


    @CLI(help ="tell tou about threads" )
    public static void thread(PrintStream out,Scanner scanner){
        CrawlerManager.getInstance().getCrawlerThreadList().stream().map(Thread::isAlive).map(e->e?1:0)
                .reduce((a,b)->a+b).map(e->"number of alive  thread "+e).ifPresent(out::println);
    }
    @CLI(help = "increase threads")
    public static void increaseThread(PrintStream out,Scanner scanner){
        CrawlerManager.getInstance().run(Integer.parseInt(scanner.nextLine()));
    }
    @CLI(help = "call gc")
    public static void gc(PrintStream out,Scanner scanner) {
        System.gc();
    }

    @CLI(help = "exit program" )
    public static void exit(PrintStream out, Scanner scanner) {
        saveDuplicate(out, scanner);
        out.println("done");
        System.exit(0);
    }

}

