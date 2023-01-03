package org.adex.demospringcloudkafka;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
public class DemoSpringCloudStreamsKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoSpringCloudStreamsKafkaApplication.class, args);
    }

}

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
class PageEvent {
    private String name;
    private String user;
    private Date date;
    private long duration;
}

@RestController
@AllArgsConstructor
class PageEventRestController {

    private StreamBridge streamBridge;

    @GetMapping("/publish/{topic}/{page}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String page) {
        PageEvent pageEvent = new PageEvent(page, Math.random() > .5 ? "U1" : "U2", new Date(), new Random().nextInt(9000));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

}

@Service
@Slf4j
class PageEventCService {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> log.warn("==> " + input);
    }

    /**
     * By default, event created in each second
     * default timer : 1sec
     * spring.integration.poller.fixed-delay=1000
     */
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(Math.random() > .5 ? "P1" : "P2", Math.random() > .5 ? "U1" : "U2", new Date(), new Random().nextInt(9000));
    }

    @Bean
    public Function<PageEvent, PageEvent> pageEventFunction() {
        return input -> {
            input.setName(input.getName().toLowerCase() + " V2");
            return input;
        };
    }

}