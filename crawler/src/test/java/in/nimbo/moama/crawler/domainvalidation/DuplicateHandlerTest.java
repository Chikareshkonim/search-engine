package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DuplicateHandlerTest {

    @Test
    public void isDuplicate() throws IOException {
        ConfigManager.getInstance().load(DuplicateHandler.class.getResourceAsStream("/test.properties"),ConfigManager.FileType.PROPERTIES);
        Assert.assertFalse(DuplicateHandler.getInstance().isDuplicate("salam"));
    }
    @Test
    public void weakConfirm() throws IOException {
        ConfigManager.getInstance().load(DuplicateHandler.class.getResourceAsStream("/test.properties"),ConfigManager.FileType.PROPERTIES);
        DuplicateHandler.getInstance().weakConfirm("salamsalam");
        Assert.assertTrue(        DuplicateHandler.getInstance().isDuplicate("salamsalam"));
    }

}