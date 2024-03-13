package org.loadbalancer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "backend.server")
public class ServerConfig {

    private List<String> id;
    private List<String> address;

    public List<String> getId() {
        return id;
    }

    public void setId(List<String> id) {
        this.id = id;
    }

    public List<String> getAddress() {
        return address;
    }

    public void setAddress(List<String> address) {
        this.address = address;
    }


    public void addServer(String id, String address)
    {
        this.getId().add(id);
        this.getAddress().add(address);
    }

    @Bean
    public RestTemplate restTemplate()
    {
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectionRequestTimeout(1000);
        httpRequestFactory.setConnectTimeout(1000);
        
        return new RestTemplate(httpRequestFactory);
    }
}
