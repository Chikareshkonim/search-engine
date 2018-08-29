package in.nimbo.moama.kafka;

import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;

public class MoamaProducer {
    private String topic;
    private Producer<String, String> producer;
    private Logger errorLogger = Logger.getLogger(this.getClass());
    public MoamaProducer(String topic) {
        //TODO
        this.topic = topic;
        producer = new KafkaProducer<>(ConfigManager.getInstance().getProperties("kafka.",true));
    }
    public void pushDocument(String document){
        producer.send(new ProducerRecord<>(topic,"", document));
    }
    public void pushNewURL(String... links) {
        System.out.println(producer.metrics());
//        for (String url : links) {
//            try {
//                String key = new URL(url).getHost();
//                producer.send(new ProducerRecord<>(topic, key, url));
//            } catch (MalformedURLException e) {
//                System.out.println(e.getMessage());
//                errorLogger.error("Wrong Exception" + url);
//            }
//            producer.flush();
//        }
    }
}
