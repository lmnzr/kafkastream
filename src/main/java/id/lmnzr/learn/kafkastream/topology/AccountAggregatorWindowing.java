package id.lmnzr.learn.kafkastream.topology;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
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
public class AccountAggregatorWindowing {
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public void process(StreamsBuilder builder) {

        // serde's
        final Serde<String> stringSerde = Serdes.String();
        final Serde<AggregatedAccount> jsonSerdeOut = CustomJsonSerde.getAggregatedAccount();
        final Serde<Transaction> jsonSerdeIn = CustomJsonSerde.getTransaction();

        // read from input topic
        KStream<String, String> inputStream = builder
            .stream("transaction");

        // stream topology
        KStream<String, String> outputStream = inputStream
            .map(this::inputToJson)
            .groupBy(
                (k, v) -> v.getCompanyId() + "#" + v.getAccountId(),
                Grouped.with("company-account", stringSerde, jsonSerdeIn)
            )
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(5)))
            .aggregate(
                AggregatedAccount::initial,
                (k, v, a) -> aggregate(v, a),
                Materialized.<String, AggregatedAccount, WindowStore<Bytes, byte[]>>as(
                        "aggregation")
                    .withValueSerde(jsonSerdeOut)
                    .withKeySerde(stringSerde)
            )
            .suppress(Suppressed.untilWindowCloses(unbounded()))
            .toStream()
            .map(this::windowToOutput);

        // write to output topic
        outputStream.to("aggregated-account");
    }

    private static AggregatedAccount aggregate(Transaction detail, AggregatedAccount aggregated) {
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

    private KeyValue<String, Transaction> inputToJson(String key, String value) {
        Transaction data = null;
        try {
            data = mapper.readValue(value, Transaction.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new KeyValue<>(key, data);
    }

    private KeyValue<String, String> windowToOutput(
        Windowed<String> windowedId, AggregatedAccount value
    ) {
        KeyValue<String, String> keyValue = null;
        try {
            keyValue = new KeyValue<>(String.valueOf(windowedId.key()),
                mapper.writeValueAsString(value)
            );
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return keyValue;
    }
}
