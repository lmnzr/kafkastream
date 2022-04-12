package id.lmnzr.learn.kafkastream.topology;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import id.lmnzr.learn.kafkastream.model.AggregatedAccount;
import id.lmnzr.learn.kafkastream.model.Transaction;
import id.lmnzr.learn.kafkastream.streamcomponent.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class AccountAggregator {
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public void process(StreamsBuilder builder) {

        // serde's
        final Serde<String> stringSerde = Serdes.String();
        final Serde<AggregatedAccount> jsonSerdeOut = CustomJsonSerde.getAggregatedAccount();
        final Serde<Transaction> jsonSerdeIn = CustomJsonSerde.getTransaction();

        // read from input topic
        KStream<String, String> inputStream = builder
            .stream("demo-transaction");

        // stream topology
        KStream<String, String> outputStream = inputStream
            .map(this::inputToJson)
            .toTable(Materialized
                .<String, Transaction, KeyValueStore<Bytes, byte[]>>as("demo-transaction-totable")
                .withKeySerde(stringSerde).withValueSerde(jsonSerdeIn))
            .groupBy(
                (k, v) -> KeyValue.pair(v.getCompanyId() + "#" + v.getAccountId(), v),
                Grouped.with("demo-company-account", stringSerde, jsonSerdeIn)
            )
            .aggregate(
                AggregatedAccount::initial,
                (k, v, a) -> adder(v, a),
                (k, v, a) -> substractor(v, a),
                Materialized
                    .<String, AggregatedAccount, KeyValueStore<Bytes, byte[]>>as("demo-aggregation")
                    .withValueSerde(jsonSerdeOut).withKeySerde(stringSerde)
            )
            .suppress(
                Suppressed.untilTimeLimit(Duration.ofMinutes(1),
                    Suppressed.BufferConfig.unbounded()
                ))
            .toStream()
            .map(this::jsonToOutput);

        // write to output topic
        outputStream.to("demo-aggregated-account");
    }

    private static AggregatedAccount adder(Transaction detail, AggregatedAccount aggregated) {
        AggregatedAccount aggregateData = new AggregatedAccount();
        List<Long> aggIds = aggregated.getIds();
        aggIds.add(detail.getId());
        aggregateData.setIds(aggIds);
        aggregateData.setCompanyId(detail.getCompanyId());
        aggregateData.setAccountId(detail.getAccountId());
        aggregateData.setCredit(aggregated.getCredit().add(detail.getCredit()));
        aggregateData.setDebit(aggregated.getDebit().add(detail.getDebit()));
        return aggregateData;
    }

    private static AggregatedAccount substractor(Transaction detail, AggregatedAccount aggregated) {
        AggregatedAccount aggregateData = new AggregatedAccount();
        List<Long> aggIds = aggregated.getIds();
        aggIds.remove(detail.getId());
        aggregateData.setIds(aggIds);
        aggregateData.setCompanyId(detail.getCompanyId());
        aggregateData.setAccountId(detail.getAccountId());
        aggregateData.setCredit(aggregated.getCredit().subtract(detail.getCredit()));
        aggregateData.setDebit(aggregated.getDebit().subtract(detail.getDebit()));
        return aggregateData;
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

    private KeyValue<String, String> jsonToOutput(
        String key, AggregatedAccount value
    ) {
        KeyValue<String, String> keyValue = null;
        try {
            keyValue = new KeyValue<>(key, mapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return keyValue;
    }
}


