package in.nimbo.moama;


import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.Crawler;
import in.nimbo.moama.kafka.MoamaProducer;

import java.io.IOException;
import java.io.InputStream;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class App {
    public static void main(String[] args) throws IOException {
        InputStream fileInputStream = Crawler.class.getResourceAsStream("/test.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        Initializer.initialize();
        Listener.listen();
        new Crawler().run();
    }
}
