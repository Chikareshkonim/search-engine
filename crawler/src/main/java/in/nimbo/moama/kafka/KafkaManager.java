package in.nimbo.moama.kafka;

import in.nimbo.moama.ConfigManager;
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

import static in.nimbo.moama.util.Constants.POLL_TIMEOUT;
import static in.nimbo.moama.util.PropertyType.*;
import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;

public class KafkaManager implements URLQueue {
    private final String topic;
    private KafkaConsumer<String, String> consumer;
    private Producer<String, String> producer;
    private DuplicateLinkHandler duplicateLinkHandler;
    private Logger errorLogger = Logger.getLogger("error");

    public KafkaManager(ConfigManager configManager, String topic) {
        this.topic = topic;
        Properties properties = configManager.getProperties("kafka", true);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        producer = new KafkaProducer<>(properties);
        duplicateLinkHandler = DuplicateLinkHandler.getInstance();
    }

    public KafkaManager(String topic, String portsWithIp, String groupID, int maxPoll) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVER, portsWithIp);
        props.put(GROUP_ID, groupID);
        props.put(ENABLE_AUTO_COMMIT, "true");
        props.put(AUTO_COMMIT_INTERVAL_MS, "10000");
        props.put(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(MAX_POLL_RECORDS, maxPoll);
        props.put(AUTO_OFFSET_RESET, "earliest");
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
