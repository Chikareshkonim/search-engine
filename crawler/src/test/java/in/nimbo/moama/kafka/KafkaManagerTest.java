package in.nimbo.moama.kafka;

import in.nimbo.moama.WebDocument;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaManagerTest {

    @Test
    public void documentToJson() {
        KafkaManager kafkaManager = new KafkaManager("");
        System.out.println(kafkaManager.documentToJson(new WebDocument()));
    }
}