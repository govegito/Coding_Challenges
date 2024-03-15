package org.loadbalancer.config;

import org.loadbalancer.service.BalancingStrategy;
import org.loadbalancer.service.ConsistentHashingStrategy;
import org.loadbalancer.service.RoundRobinStrategy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BalancingStrategyFactory {

    @Bean
    @ConditionalOnProperty(value = "strategy.name",havingValue = "ConsistentHashing")
    public BalancingStrategy strategy(ConsistentHashingStrategy strategy)
    {
        return strategy;
    }

    @Bean
    @ConditionalOnProperty(value = "strategy.name",havingValue = "RoundRobin")
    public BalancingStrategy strategy2(RoundRobinStrategy strategy)
    {
        return strategy;
    }
}
