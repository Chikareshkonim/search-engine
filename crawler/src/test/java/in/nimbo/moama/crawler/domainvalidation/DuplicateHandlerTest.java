package in.nimbo.moama.crawler.domainvalidation;

import in.nimbo.moama.Initializer;
import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;

public class DuplicateHandlerTest {

    @Test
    public void isDuplicate() throws IOException {
        ConfigManager.getInstance().load(DuplicateHandler.class.getResourceAsStream("/crawler.properties"),ConfigManager.FileType.PROPERTIES);
        Assert.assertFalse(DuplicateHandler.getInstance().isDuplicate("salam"));
        Assert.assertTrue(DuplicateHandler.getInstance().isDuplicate("http://www.EOFire.com/"));
    }
    @Test
    public void weakConfirm() throws IOException {
        ConfigManager.getInstance().load(DuplicateHandler.class.getResourceAsStream("/crawler.properties"),ConfigManager.FileType.PROPERTIES);
        DuplicateHandler.getInstance().weakConfirm("salamsalam");
        Assert.assertTrue(DuplicateHandler.getInstance().isDuplicate("salamsalam"));
    }

    @Test
    public void getInstance() {
    }

    @Test
    public void weakCheckDuplicate() {
    }

    @Test
    public void bulkNotDuplicate() throws IOException, URISyntaxException {
        Initializer.initialize();
        LinkedList<String> linkedList=new LinkedList<>();
        linkedList.add("salam");
        linkedList.add("khodahafez");
        DuplicateHandler.getInstance().bulkNotDuplicate(linkedList).forEach(System.out::println);
    }
}