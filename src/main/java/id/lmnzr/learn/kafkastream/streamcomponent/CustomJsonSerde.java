package id.lmnzr.learn.kafkastream.streamcomponent;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import id.lmnzr.learn.kafkastream.model.AggregatedAccount;
import id.lmnzr.learn.kafkastream.model.Transaction;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CustomJsonSerde {

    public static Serde<AggregatedAccount> getAggregatedAccount() {
        Serializer<AggregatedAccount> jsonSerializer = new JsonSerializer<>();
        Deserializer<AggregatedAccount> jsonDeserializer = new JsonDeserializer<>();
        Map<String, Object> props = getJsonSerdeProperties();
        props.put(
            JsonDeserializer.VALUE_DEFAULT_TYPE,
            "id.lmnzr.learn.kafkastream.model.AggregatedAccount"
        );

        jsonDeserializer.configure(props, false);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Transaction> getTransaction() {
        Serializer<Transaction> jsonSerializer = new JsonSerializer<>();
        Deserializer<Transaction> jsonDeserializer = new JsonDeserializer<>();
        Map<String, Object> props = getJsonSerdeProperties();
        props.put(
            JsonDeserializer.VALUE_DEFAULT_TYPE,
            "id.lmnzr.learn.kafkastream.model.Transaction"
        );

        jsonDeserializer.configure(props, false);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    private static Map<String, Object> getJsonSerdeProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "id.lmnzr.learn.kafkastream");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
        return props;
    }
}
