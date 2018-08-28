package in.nimbo.moama;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class WebDocumentHBaseManagerTest {

    @Test
    public void createTableTest() {
        WebDocumentHBaseManager webDocumentHBaseManager = new WebDocumentHBaseManager("test","1","2");
        assertTrue(webDocumentHBaseManager.createTable());
    }
}