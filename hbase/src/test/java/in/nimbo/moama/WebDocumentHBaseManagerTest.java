package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class WebDocumentHBaseManagerTest {

    @Test
    public void createTableTest() {
        try {
            ConfigManager.getInstance().load(WebDocumentHBaseManager.class.getResourceAsStream("/config.properties"), ConfigManager.FileType.PROPERTIES);
        } catch (IOException e) {
            e.printStackTrace();
        }
        WebDocumentHBaseManager webDocumentHBaseManager = new WebDocumentHBaseManager("test","1","2");
        assertTrue(webDocumentHBaseManager.createTable());
    }
}