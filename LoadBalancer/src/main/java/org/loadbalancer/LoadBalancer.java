package org.loadbalancer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LoadBalancer {
    public static void main(String[] args) {

        SpringApplication app = new SpringApplication(LoadBalancer.class);
        app.run(args);
    }
}