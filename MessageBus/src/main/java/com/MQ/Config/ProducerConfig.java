package com.MQ.Config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "producer")
public class ProducerConfig {

    private int brokerNumber;

    private int partitionNumber;

    public ProducerConfig() {
    }

    public ProducerConfig(int brokerNumber, int partitionNumber) {
        this.brokerNumber = brokerNumber;
        this.partitionNumber = partitionNumber;
    }

    public int getBrokerNumber() {
        return brokerNumber;
    }

    public void setBrokerNumber(int brokerNumber) {
        this.brokerNumber = brokerNumber;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }
}
