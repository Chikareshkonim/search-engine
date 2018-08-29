package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class NewsWebsiteHBaseManagerTest {
    private NewsWebsiteHBaseManager hBaseManager;

    @BeforeClass
    public static void beforeClass() throws IOException {
        ConfigManager.getInstance().load(NewsWebsiteHBaseManager.class.getResourceAsStream("/config.properties"),
                ConfigManager.FileType.PROPERTIES);

    }

    @Before
    public void setUp() {
       hBaseManager  = new NewsWebsiteHBaseManager("newsWebsites", "template", "rss");

    }

    @Test
    public void createTable() throws IOException {
//        assertTrue(hBaseManager.createTable());
    }

    @Test
    public void getTemplates() {
//        System.out.println(hBaseManager.getTemplates());
        assertEquals("[{\"attModel\":\"class\",\"attValue\":\"css-18sbwfn StoryBodyCompanionColumn\"," +
                "\"dateFormat\":\"EEE, dd MMM yyyy HH:mm:ss z\",\"domain\":\"nytimes.com\",\"newsTag\":\"link\"}]",
                hBaseManager.getTemplates().toString());
    }

    @Test
    public void getRSSList() {
//        System.out.println(hBaseManager.getRSSList());
        assertEquals("[{\"rss\":\"http://rss.nytimes.com/services/xml/rss/nyt/World.xml\"" +
                ",\"domain\":\"nytimes.com\"}]",hBaseManager.getRSSList().toString());
    }
}