package in.nimbo.moama;


import in.nimbo.moama.crawler.Crawler;
import in.nimbo.moama.kafka.MoamaProducer;

public class CrawlerApp {
    public static void main(String[] args) {
        Initializer.initialize();
        Thread crawl = new Thread(new Crawler());
        crawl.start();
        new Thread(Listener::listen).start();
    }
}
