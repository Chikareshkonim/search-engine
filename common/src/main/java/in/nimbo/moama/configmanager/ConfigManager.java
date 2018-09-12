package in.nimbo.moama.configmanager;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfigManager {
    private Properties properties;
    private static final ConfigManager ourInstance = new ConfigManager();
    private InputStream fileInputStream;
    private FileType fileType;

    private ConfigManager() {
    }

    public static ConfigManager getInstance() {
        return ourInstance;
    }

    public void load(InputStream InputStream, FileType fileType) throws IOException {
        this.fileInputStream = InputStream;
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

    public void load(FileInputStream fileInputStream) throws IOException {
        properties.clear();
        load(this.fileInputStream, fileType);
    }

    public String getProperty(PropertyType type) {
        return properties.getProperty(type.toString());
    }

    public int getIntProperty(PropertyType type) {
        return Integer.parseInt(properties.getProperty(type.toString()));
    }
    public long getLongProperty(PropertyType type) {
        return Long.parseLong(properties.getProperty(type.toString()));
    }
    public double getDoubleProperty(PropertyType type) {
        return Double.parseDouble(properties.getProperty(type.toString()));
    }


    public String getProperty(PropertyType type, String extendRoot) {
        return properties.getProperty(extendRoot + type.toString());
    }

    public void refresh() {
    }

    public Properties getProperties(PropertyType root, boolean mustCutRoot) {
        return getProperties(root.toString(), mustCutRoot);
    }

    public Properties getProperties(String root, boolean mustCutRoot) {
        Properties tempProperties = new Properties();
        properties.entrySet().stream().filter(entry -> ((String) entry.getKey()).startsWith(root))
                .forEach(entry -> tempProperties.setProperty(((String) entry.getKey()).substring(mustCutRoot ? root.length() : 0), (String) entry.getValue()));
        return tempProperties;
    }

    public enum FileType {
        XML, PROPERTIES
    }
}
