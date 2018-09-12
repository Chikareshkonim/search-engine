package in.nimbo.moama.kafka;

import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class KafkaTest {
    @Test
    public void testKafka() throws IOException {
        InputStream fileInputStream = KafkaTest.class.getResourceAsStream("/config.properties");
        ConfigManager.getInstance().load(fileInputStream, PROPERTIES);
        MoamaConsumer moamaConsumer = new MoamaConsumer("test","kafka.");
        MoamaProducer moamaProducer = new MoamaProducer("test","kafka.");
        ArrayList<String> results = new ArrayList<>();
        List<String> links = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            for (int j = 1; j < 61; j++) {
                links.add("https://wwww.testMessage" + i + "_" + j);
            }
            moamaProducer.pushNewURL(links.toArray(new String[0]));
            moamaProducer.flush();
            int lastSize = results.size();
            for (String link : moamaConsumer.getDocuments()) {
                System.out.println(i + " " + link);
                results.add(link);
            }
            Assert.assertEquals(30, results.size() - lastSize);
            lastSize = results.size();
            System.out.println("First poll! " + i);
            for (String link :  moamaConsumer.getDocuments()) {
                System.out.println(i + " " + link);
                results.add(link);
            }
            Assert.assertEquals(30, results.size() - lastSize);
            System.out.println("Second poll! " + i);
            moamaConsumer.getDocuments();
        }
    }
}