package in.nimbo.moama.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;


public class MoamaConsumerTest {

    @Test
    public void getDocuments() {
        Producer<String,String> producer= new MockProducer<>();
        producer.send(new ProducerRecord<>("salam","imok"));
        producer.send(new ProducerRecord<>("salam","imok"));
        producer.send(new ProducerRecord<>("salam","imok"));
        producer.send(new ProducerRecord<>("salam","imok"));
        producer.send(new ProducerRecord<>("salam","imok"));
        producer.send(new ProducerRecord<>("salam","imok"));
        producer.flush();
        Consumer consumer = new MockConsumer<String,String>(OffsetResetStrategy.EARLIEST);
        consumer.subscribe(Collections.singletonList("salam"));
        ArrayList<String> result = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll(10000);
        consumer.commitSync();
        for (ConsumerRecord<String, String> record : records) {
            result.add(record.value());
        }
        result.forEach(System.out::println);

    }
}