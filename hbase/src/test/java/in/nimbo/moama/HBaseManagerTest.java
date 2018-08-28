package in.nimbo.moama;

import com.google.protobuf.ServiceException;
import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class HBaseManagerTest {

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
    public void constructorTest() throws Exception {
        ConfigManager.getInstance().load(HBaseManager.class.getResourceAsStream("/config.properties"), ConfigManager.FileType.PROPERTIES);
        HBaseManager hBaseManager = new HBaseManager("test","test");
        throw new Exception();
    }
}