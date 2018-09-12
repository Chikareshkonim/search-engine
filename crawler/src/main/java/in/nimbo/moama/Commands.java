package in.nimbo.moama;

import in.nimbo.moama.crawler.CrawlThread;
import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.crawler.PageFetcher;
import in.nimbo.moama.news.listener.CLI;
import in.nimbo.moama.metrics.Metrics;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Commands {
    @CLI(help = "shows you  how much thread is in each States")
    public static void states(PrintStream out, Scanner scanner) {
        try {
            out.println(PageFetcher.getInstance().consumeState);
            PageFetcher.getInstance().fetchers.stream()
                    .map(Thread::toString)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .forEach((k, v) -> out.println(k + "   " + v));

        } catch (RuntimeException e) {
            e.printStackTrace(out);
        }
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

    @CLI(help = "call gc")
    public static void gc(PrintStream out, Scanner scanner) {
        try {
            System.gc();
        } catch (Exception e) {
            e.printStackTrace(out);
        }

    }

    @CLI(help = "fatal errors")
    public static void fatal(PrintStream out, Scanner scanner) {
        try {
            CrawlThread.fatalErrors.forEach(out::println);
        } catch (Exception e) {
            e.printStackTrace(out);
        }

    }


    @CLI(help = "exit program")
    public static void exit(PrintStream out, Scanner scanner) {
        try {
            thread(out, scanner);
            CrawlerManager.getInstance().setRun(false);
            CrawlerManager.getInstance().getCrawlerThreadList().forEach(CrawlThread::off);
            Utils.delay(20000);
            CrawlThread.exiting();
            Utils.delay(20000);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace(out);
        }

    }

}

