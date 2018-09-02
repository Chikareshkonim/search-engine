package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.rankcalculator.ReferenceCalculator;
import in.nimbo.moama.util.ReferencePropertyType;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class App {
    private static final Logger errorLogger = Logger.getLogger(ReferenceCalculator.class);
    public static void main(String[] args) {
        InputStream fileInputStream = App.class.getResourceAsStream("/config.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        try {
            configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        } catch (IOException e) {
            errorLogger.error("Loading properties failed!");
        }
        ReferenceCalculator referenceCalculator;
        referenceCalculator = new ReferenceCalculator(configManager.getProperty(ReferencePropertyType.APP_NAME),configManager.getProperty(ReferencePropertyType.MASTER_ADDRESS));
        referenceCalculator.calculate();
    }
}
