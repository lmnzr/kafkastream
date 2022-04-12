package id.lmnzr.learn.kafkastream.topology;


import java.time.Duration;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import id.lmnzr.learn.kafkastream.model.AggregatedAccount;
import id.lmnzr.learn.kafkastream.model.Transaction;
import id.lmnzr.learn.kafkastream.processor.AccountAggregatorProcessor;
import id.lmnzr.learn.kafkastream.streamcomponent.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;

//@Component
@Slf4j
public class AccountAggregatorV2 {
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public void process(StreamsBuilder builder) {

        // serde's
        final Serde<String> stringSerde = Serdes.String();
        final Serde<AggregatedAccount> jsonSerdeOut = CustomJsonSerde.getAggregatedAccount();

        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("aggregation-state-store"),
                    stringSerde, jsonSerdeOut
                )
                .withLoggingDisabled().withCachingDisabled());

        // read from input topic
        KStream<String, String> inputStream = builder
            .stream("transaction");

        // stream topology
        inputStream
            .map(this::inputToJson)
            .transform(()->new AccountAggregatorProcessor(Duration.ofMinutes(5)))
            .foreach(this::writeToSink);

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

    /**
     * Placeholder method for simulating a database write operation
     * @param key
     * @param value
     */
    private void writeToSink(String key, AggregatedAccount value) {
        //Placeholder method for simulating a write operation to an external sink.
        log.info("Persisting to Sink : {} {}", key, value);
    }
}
