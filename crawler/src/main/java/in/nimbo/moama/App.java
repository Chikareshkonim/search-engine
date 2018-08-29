package in.nimbo.moama;


import in.nimbo.moama.crawler.Crawler;

public class App {
    public static void main(String[] args) {
        Initializer.initialize();
        Thread crawl = new Thread(new Crawler());
        crawl.start();
        new Thread(Listener::listen).start();
    }
}
