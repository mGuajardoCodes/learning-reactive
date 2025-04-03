package cl.tenpo.learning.reactive.tasks.task1;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;

@Component
@RequiredArgsConstructor
@Slf4j
public class T1Question6 {

    Random random = new Random();

    public ConnectableFlux<Double> question6() {
        // Flux that emits a value every 500ms and maps each tick to a random price between 1 and 500
        return Flux.interval(Duration.ofMillis(500))
                .doOnSubscribe(subscription -> log.info("Starting question6: flux subscribed"))
                .map(tick -> (double) (random.nextInt(500) + 1))
                .doOnNext(price -> log.info("Question6 emitted price: {}", price))
                .publish();
                // reminder :p -> publish() converts the flux to a "hot" stream, so that subscribers only receive data emitted after connection
    }

}
