package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class HBaseManagerTest {
    @BeforeClass
    public static void beforeClass() throws IOException {
        ConfigManager.getInstance().load(NewsWebsiteHBaseManager.class.getResourceAsStream("/config.properties"),
                ConfigManager.FileType.PROPERTIES);

    }

    @Test
    public void connectionTest() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(getClass().getResourceAsStream("/hbase-site.xml"));
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);
        } catch (ServiceException | IOException e) {
            throw new Exception();
        }
    }

    @Test
    public void constructorTest() {
        HBaseManager hBaseManager = new HBaseManager("test", "test");
    }

    @Test
    public void isDuplicate() {
        HBaseManager hBaseManager = new HBaseManager("pages", "score");
        WebDocumentHBaseManager webDocumentHBaseManager =new WebDocumentHBaseManager("pages","outLinks","score");

        Assert.assertTrue(hBaseManager.isDuplicate("https://en.wikipedia.org/wiki/Main_Page"));
    }
}