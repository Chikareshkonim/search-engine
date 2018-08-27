package in.nimbo.moama.kafka;

import in.nimbo.moama.ConfigManager;
import kafka.utils.Json;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

public class KafkaProducer {
    private String topic;
    private Producer<String, Json> producer;
    private Logger errorLogger = Logger.getLogger("error");
    public KafkaProducer(String topic) {
        //TODO
        this.topic = topic;
        ConfigManager configManager = new ConfigManager();
        producer = new KafkaProducer(configManager.getProperties("",true));
    }
    public void push(Json document){
        producer.send(new ProducerRecord<>(topic, document));
    }
}
