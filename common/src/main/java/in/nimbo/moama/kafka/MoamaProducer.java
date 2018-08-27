package in.nimbo.moama.kafka;

import in.nimbo.moama.configmanager.ConfigManager;
import kafka.utils.Json;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class MoamaProducer {
    private String topic;
    private Producer<String, String> producer;
    private Logger errorLogger = Logger.getLogger("error");
    public MoamaProducer(String topic, String propertiesAddress) {
        //TODO
        this.topic = topic;
        ConfigManager configManager = null;
        try {
            configManager = new ConfigManager(propertiesAddress, PROPERTIES);
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer = new KafkaProducer<>(configManager.getProperties("",true));
    }
    public void pushDocument(String document){
        producer.send(new ProducerRecord<>(topic,"", document));
    }
    public void pushNewURL(String... links) {
        for (String url : links) {
            try {
                String key = new URL(url).getHost();
                producer.send(new ProducerRecord<>(topic, key, url));
            } catch (MalformedURLException e) {
                errorLogger.error("Wrong Exception" + url);
            }
        }

    }
}
