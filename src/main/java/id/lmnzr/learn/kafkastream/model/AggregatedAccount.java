package id.lmnzr.learn.kafkastream.model;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class AggregatedAccount {
    @JsonProperty("ids")
    private List<Long> ids;

    @JsonProperty("companyId")
    private Long companyId;

    @JsonProperty("accountId")
    private Long accountId;

    @JsonProperty("debit")
    private BigDecimal debit;

    @JsonProperty("credit")
    private BigDecimal credit;

    public static AggregatedAccount initial(){
        AggregatedAccount initialData = new AggregatedAccount();
        initialData.setDebit(BigDecimal.ZERO);
        initialData.setCredit(BigDecimal.ZERO);
        initialData.setIds(new ArrayList<>());
        return initialData;
    }
}
