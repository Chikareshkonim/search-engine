package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.rankcalculator.ReferenceCalculator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class App {
    private static final Logger LOGGER = Logger.getLogger(ReferenceCalculator.class);
    public static void main(String[] args) throws URISyntaxException {
        InputStream fileInputStream = App.class.getResourceAsStream("/config.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        try {
            configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        } catch (IOException e) {
            LOGGER.error("Loading properties failed!");
        }
        ReferenceCalculator referenceCalculator;
        referenceCalculator = new ReferenceCalculator("reference","spark://s2:7077");
        referenceCalculator.calculate();
    }
}
