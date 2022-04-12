package id.lmnzr.learn.kafkastream.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Transaction {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("companyId")
    private Long companyId;

    @JsonProperty("accountId")
    private Long accountId;

    @JsonProperty("debit")
    private BigDecimal debit;

    @JsonProperty("credit")
    private BigDecimal credit;


}
