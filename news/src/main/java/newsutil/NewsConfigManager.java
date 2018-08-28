package newsutil;

import in.nimbo.moama.configmanager.ConfigManager;

import java.io.File;
import java.io.IOException;

public class NewsConfigManager {
    private ConfigManager configManager;
    private static NewsConfigManager ourInstance = new NewsConfigManager();

    public static NewsConfigManager getInstance() {
        return ourInstance;
    }

    private NewsConfigManager() {
        try {
            configManager = new ConfigManager(new File(NewsConfigManager.class.getResource("/news.properties").getFile())
                    .getAbsolutePath(), ConfigManager.FileType.PROPERTIES);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(NewsPropertyType propertyType) {
        return configManager.getProperty(propertyType);
    }
}
