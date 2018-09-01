package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.rankcalculator.ReferenceCalculator;

import java.io.IOException;
import java.io.InputStream;

public class App {
    public static void main(String[] args) {
        InputStream fileInputStream = App.class.getResourceAsStream("/config.properties");
        ConfigManager configManager=ConfigManager.getInstance();
        try {
            configManager.load(fileInputStream,ConfigManager.FileType.PROPERTIES);
        } catch (IOException e) {
            System.out.println("Loading properties failed!");
        }
        ReferenceCalculator referenceCalculator;
        referenceCalculator = new ReferenceCalculator("refrence","spark://s2:7077");
        referenceCalculator.calculate();
    }
}
