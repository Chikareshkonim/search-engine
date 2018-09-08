package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import org.json.JSONArray;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, URISyntaxException {
        String recourseAddress = new File(App.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
        if (recourseAddress.endsWith(".jar")) {
            recourseAddress = recourseAddress.substring(0, recourseAddress.lastIndexOf("/")).concat("/../src/main/resources/");
        } else {
            recourseAddress = recourseAddress.concat("/../../src/main/resources/");
        }
        FileInputStream configInputStream = new FileInputStream(recourseAddress + "keyword.properties");
        ConfigManager configManager = ConfigManager.getInstance();
        configManager.load(configInputStream, ConfigManager.FileType.PROPERTIES);
        KeywordFinder keywordFinder = new KeywordFinder();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(keywordFinder,0,1, TimeUnit.MILLISECONDS);
    }
}
