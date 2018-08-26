package in.nimbo.moama.kafka;

import in.nimbo.moama.crawler.URLQueue;
import in.nimbo.moama.crawler.domainvalidation.DuplicateLinkHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class KafkaManager implements URLQueue {
    public static final int POLL_TIMEOUT = 10000;
    private final String topic;
    private KafkaConsumer<String, String> consumer;
    private Producer<String, String> producer;
    private DuplicateLinkHandler duplicateLinkHandler;
    private Logger errorLogger = Logger.getLogger("error");

    public KafkaManager(String topic, String portsWithIp, String groupID, int maxPoll) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", portsWithIp);
        props.put("group.id", groupID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("max.poll.records", maxPoll);
        props.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        producer = new KafkaProducer<>(props);
        duplicateLinkHandler = DuplicateLinkHandler.getInstance();
        try {
            duplicateLinkHandler.loadHashTable();
        } catch (IOException e) {
            errorLogger.error("vay vay vay ,cant create kafka objects");
            System.exit(0);
        }
    }

    @Override
    public synchronized ArrayList<String> getUrls() {
        ArrayList<String> result = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        consumer.commitSync();
        for (ConsumerRecord<String, String> record : records) {
            result.add(record.value());
        }
        return result;
    }

    @Override
    public void pushNewURL(String... links) {
        for (String url : links) {
            try {
                String key = new URL(url).getHost();
                producer.send(new ProducerRecord<>(topic, key, url));
            } catch (MalformedURLException e) {
                errorLogger.error("Wrong Exception" + url);
            }
//            }
        }

    }


    @Override
    protected void finalize() {
        flush();
        producer.close();
        consumer.close();
        duplicateLinkHandler.saveHashTable();
    }

    public void flush() {
        producer.flush();
    }
}
