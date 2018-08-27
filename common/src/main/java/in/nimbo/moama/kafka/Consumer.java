package in.nimbo.moama.kafka;

import in.nimbo.moama.configmanager.ConfigManager;
import kafka.utils.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;

public class Consumer {
    private KafkaConsumer<String, String> consumer;
    private Logger errorLogger = Logger.getLogger("error");

    public Consumer(String topic, String propertiesAddress) {
        //TODO
        ConfigManager configManager = null;
        try {
            configManager = new ConfigManager(propertiesAddress, PROPERTIES);
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumer = new KafkaConsumer<>(configManager.getProperties("",true));
        consumer.subscribe(Collections.singletonList(topic));
    }
    public synchronized ArrayList<String> getDocument() {
        //TODO
        ArrayList<String> result = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll(10000);
        consumer.commitSync();
        for (ConsumerRecord<String, String> record : records) {
            result.add(record.value());
        }
        return result;
    }
}
