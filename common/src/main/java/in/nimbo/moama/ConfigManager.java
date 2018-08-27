package in.nimbo.moama;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
public class ConfigManager {
    private final String fileAddress;
    private final FileType fileType;
    private Properties properties;

    public ConfigManager(String fileAddress, FileType fileType) throws IOException {
        this.fileAddress = fileAddress;
        this.fileType = fileType;
        properties = new Properties();
        load();
    }

    private void load() throws IOException {
        switch (fileType) {
            case PROPERTIES:
                properties.load(new FileInputStream(fileAddress));
                break;
            case XML:
                properties.loadFromXML(new FileInputStream(fileAddress));
                break;
        }
    }

    public String getProperty(PropertyType type) {
        return properties.getProperty(type.toString());
    }

    public String getProperty(PropertyType type, String extendRoot) {
        return properties.getProperty(extendRoot + type.toString());
    }

    public void refresh() throws IOException {
        properties.clear();
        load();
    }
    public Properties getProperties(String root, boolean cutRoot) {
        Properties tempProperties = new Properties();
        properties.entrySet().stream().filter(entry -> ((String) entry.getKey()).startsWith(root))
                .forEach(entry -> tempProperties.setProperty(
                        ((String) entry.getKey()).substring(cutRoot ? root.length() : 0), (String) entry.getValue()));
        return tempProperties;
    }

    public static void printProperties(Properties properties) {
        properties.forEach((k, v) -> System.out.println(k + "    " + v));
    }

    public enum FileType {
        XML, PROPERTIES
    }
}
