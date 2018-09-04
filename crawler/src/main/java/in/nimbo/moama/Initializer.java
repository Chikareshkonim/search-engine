package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.CrawlerManager;
import in.nimbo.moama.crawler.Parser;
import in.nimbo.moama.crawler.language.LangDetector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Initializer {
    public static void initialize() throws IOException {
        InputStream fileInputStream = new FileInputStream(System.getProperty("user.dir")+
                "/crawler/src/main/resources/crawler.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        LangDetector langDetector = LangDetector.getInstance();
        Parser.setLangDetector(langDetector);
        langDetector.profileLoad();
        System.out.println("load langDetect finished");

    }
}
