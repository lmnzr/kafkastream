package id.lmnzr.learn.kafkastream.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import id.lmnzr.learn.kafkastream.model.Transaction;
import id.lmnzr.learn.kafkastream.streamcomponent.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CompanyCounter {

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public void process(StreamsBuilder builder) {

        // serde's
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<Transaction> jsonSerdeIn = CustomJsonSerde.getTransaction();

        // read from input topic
        KStream<String, String> inputStream = builder
            .stream("transaction");

        // stream topology
        KStream<String, String> outputStream = inputStream
            .map(this::inputToJson)
            .groupBy(
                (k, v) -> v.getCompanyId(),
                Grouped.with("company", longSerde, jsonSerdeIn)
            )
            .count()
            .toStream()
            .map(this::mapToString);

        // write to output topic
        outputStream.to("company-count");
    }

    private KeyValue<String, Transaction> inputToJson(String key, String value) {
        Transaction data = null;
        try {
            data = mapper.readValue(value, Transaction.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new KeyValue<>(key, data);
    }

    private KeyValue<String, String> mapToString(Long key, Long value) {
        return new KeyValue<>(String.valueOf(key), String.valueOf(value));
    }
}
