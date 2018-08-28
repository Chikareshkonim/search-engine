package in.nimbo.moama.configmanager;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigManager {
    private Properties properties;
    private static ConfigManager ourInstance = new ConfigManager();
    private InputStream fileInputStream;
    private FileType fileType;

    private ConfigManager() {
    }

    public static ConfigManager getInstance() {
        return ourInstance;
    }

    public void load(InputStream fileInputStream, FileType fileType) throws IOException {
        this.fileInputStream = fileInputStream;
        this.fileType = fileType;
        properties = new Properties();
        switch (fileType) {
            case PROPERTIES:
                properties.load(fileInputStream);
                break;
            case XML:
                properties.loadFromXML(fileInputStream);
                break;
        }
    }

    private void load() throws IOException {
        properties.clear();
        load(fileInputStream, fileType);
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
