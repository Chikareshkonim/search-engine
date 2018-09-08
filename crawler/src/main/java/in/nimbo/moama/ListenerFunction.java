package in.nimbo.moama;

import in.nimbo.moama.crawler.CrawlThread;
import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.crawler.domainvalidation.HashDuplicateChecker;
import in.nimbo.moama.listener.CLI;
import in.nimbo.moama.metrics.Metrics;

import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Scanner;

import static java.lang.Thread.sleep;

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


    @CLI(help = "tell tou about threads")
    public static void thread(PrintStream out, Scanner scanner) {
        LinkedList<CrawlThread> threadList = CrawlerManager.getInstance().getCrawlerThreadList();
        threadList.stream().map(Thread::isAlive).map(e -> e ? 1 : 0)
                .reduce((a, b) -> a + b).map(e -> "number of alive  thread " + e).ifPresent(out::println);
        threadList.stream()
                .filter(crawlThread -> !crawlThread.isAlive())
                .peek(CrawlThread::end)
                .forEach(threadList::remove);
    }

    @CLI(help = "increase threads")
    public static void increaseThread(PrintStream out, Scanner scanner) {
        out.println("how many thread you want to increase?");
        CrawlerManager.getInstance().run(Integer.parseInt(scanner.nextLine()));
    }

    @CLI(help = "decrease threads")
    public static void decreaseThread(PrintStream out, Scanner scanner) {
        out.println("how many thread you want to decrease?");
        thread(out, scanner);
        LinkedList<CrawlThread> threadList = CrawlerManager.getInstance().getCrawlerThreadList();
        threadList.stream()
                .limit(Integer.parseInt(scanner.nextLine()))
                .peek(CrawlThread::off)
                .forEach(threadList::remove);
    }

    @CLI(help = "call gc")
    public static void gc(PrintStream out, Scanner scanner) {
        System.gc();
    }
    @CLI(help = "fatal errors")
    public static void fatal(PrintStream out, Scanner scanner) {
        CrawlThread.fatalErrors.forEach(out::println);
    }

    @CLI(help = "exit program")
    public static void exit(PrintStream out, Scanner scanner) {
        thread(out, scanner);
        CrawlerManager.getInstance().setRun(false);
        CrawlerManager.getInstance().getCrawlerThreadList().forEach(CrawlThread::off);
        CrawlThread.exiting();
        Utils.delay(30000);
        System.exit(0);
    }

}

