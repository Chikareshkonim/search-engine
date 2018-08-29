package in.nimbo.moama.kafka;

import in.nimbo.moama.configmanager.ConfigManager;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;
import static org.junit.Assert.*;

public class KafkaTest {
    @Test
    public void testKafka() throws IOException {
        InputStream fileInputStream = KafkaTest.class.getResourceAsStream("/config.properties");
        ConfigManager.getInstance().load(fileInputStream, PROPERTIES);
        MoamaConsumer moamaConsumer = new MoamaConsumer("test");
        MoamaProducer moamaProducer = new MoamaProducer("test");
        ArrayList<String> results = new ArrayList<>();
        List<String> links = new ArrayList<>();
        for (int i = 1; i < 6; i++) {
            for (int j = 0; j < 100; j++) {
                links.add("https://wwww.testMessage" + i + "_" + j);
            }
            results = moamaConsumer.getDocuments();
            for(String result: results){
                System.out.println(result);
            }
            moamaProducer.pushNewURL(links.toArray(new String[0]));
            for (String link : moamaConsumer.getDocuments()) {
                System.out.println(link);
                results.add(link);
            }
            System.out.println("First poll! " + i);
            for (String link :  moamaConsumer.getDocuments()) {
                System.out.println(link);
                results.add(link);
            }
            System.out.println("Second poll! " + i);
        }
    }
}