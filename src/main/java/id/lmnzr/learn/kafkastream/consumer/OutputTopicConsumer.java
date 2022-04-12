package id.lmnzr.learn.kafkastream.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OutputTopicConsumer {

    @KafkaListener(topics = {"transaction"}, groupId = "transaction-streams-listener")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("received = " + record.value() + " with key " + record.key());
    }
}
