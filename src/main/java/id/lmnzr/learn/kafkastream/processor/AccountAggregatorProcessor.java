package id.lmnzr.learn.kafkastream.processor;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import id.lmnzr.learn.kafkastream.model.AggregatedAccount;
import id.lmnzr.learn.kafkastream.model.Transaction;

public class AccountAggregatorProcessor
    implements Transformer<String, Transaction, KeyValue<String, AggregatedAccount>>, Punctuator {

    private final Duration interval;

    private ProcessorContext ctx;
    private KeyValueStore<String, AggregatedAccount> aggregateStore;

    public AccountAggregatorProcessor(Duration interval) {
        this.interval = interval;
    }

    @Override
    public void init(ProcessorContext context) {
        this.ctx = context;
        this.aggregateStore = context.getStateStore("aggregation-state-store");
        this.ctx.schedule(interval, PunctuationType.WALL_CLOCK_TIME, this);
    }

    @Override
    public KeyValue<String, AggregatedAccount> transform(
        String s, Transaction transaction
    ) {
        KeyValue<String, AggregatedAccount> toForward = null;

        //initial
        String stateStoreKey =
            transaction.getAccountId() + "#" + transaction.getCompanyId();
        aggregateStore.putIfAbsent(stateStoreKey, AggregatedAccount.initial());

        //update
        AggregatedAccount aggregatedAccount = aggregate(transaction,
            aggregateStore.get(stateStoreKey)
        );
        aggregateStore.put(stateStoreKey, aggregatedAccount);

        toForward = KeyValue.pair(stateStoreKey, aggregatedAccount);
        return toForward;
    }

    private void forwardAll() {
        KeyValueIterator<String, AggregatedAccount> it = aggregateStore.all();
        while (it.hasNext()) {
            KeyValue<String, AggregatedAccount> entry = it.next();
            ctx.forward(entry.key, entry.value);
            aggregateStore.delete(entry.key);
        }
        it.close();
    }

    @Override
    public void punctuate(long timestamp) {
        forwardAll();
    }

    @Override
    public void close() {
        forwardAll();
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
}
