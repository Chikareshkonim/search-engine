package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.crawler.Parser;
import in.nimbo.moama.crawler.language.LangDetector;
import static java.lang.System.out;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;

public class Initializer {
    public static void initialize() throws IOException, URISyntaxException {
        String recourseAddress = getResourcesAddress();
        out.println("recourse Address is " +recourseAddress);
        FileInputStream configInputStream = new FileInputStream(recourseAddress + "crawler.properties");
        ConfigManager configManager = ConfigManager.getInstance();
        configManager.load(configInputStream, ConfigManager.FileType.PROPERTIES);
        LangDetector langDetector = LangDetector.getInstance();
        Parser.setLangDetector(langDetector);
        langDetector.profileLoad(recourseAddress + "profiles");
        out.println("load langDetect finished");
    }

    private static String getResourcesAddress() throws URISyntaxException {
        String recourseAddress = new File(LangDetector.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
        if (recourseAddress.endsWith(".jar")) {
            recourseAddress = recourseAddress.substring(0, recourseAddress.lastIndexOf("/")).concat("/../src/main/resources/");
        } else {
            recourseAddress = recourseAddress.concat("/../../src/main/resources/");
        }
        return recourseAddress;
    }
}
