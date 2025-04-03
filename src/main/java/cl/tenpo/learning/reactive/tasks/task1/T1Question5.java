package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.service.CalculatorService;
import cl.tenpo.learning.reactive.utils.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@Slf4j
@Component
@RequiredArgsConstructor
public class T1Question5 {

    private final CalculatorService calculatorService;
    private final UserService userService;

    private final static String CHUCK_NORRIS = "Chuck Norris";

    /**
     - Generates numbers from 100 to 1000 (901 elements)
     - Uses concatMap to ensure processing in ascending order; if any calculation fails, the stream stops
     - If all calculations complete without error, it proceeds to fetch a name using findFirstName
     - If any error occurs during the calculation process, it returns "Chuck Norris"
     */
    public Mono<String> question5A() {
        return Flux.range(100, 901)
                .doOnSubscribe(subscription -> log.info("Starting question5A: processing numbers 100 to 1000"))
                .concatMap(num -> calculatorService.calculate(BigDecimal.valueOf(num)))
                .then(Mono.defer(userService::findFirstName))
                .doOnError(error -> log.error("Question5A encountered an error during calculation: {}", error.getMessage()))
                .onErrorResume(e -> Mono.just(CHUCK_NORRIS))
                .doOnSuccess(result -> log.info("Finished question5A with result: {}", result));
    }

    /**
     - Generates numbers from 100 to 1000 (901 elements)
     - Uses flatMap along with collectList to wait for all calculations to complete
     - If calculations complete without errors, it calls findAllNames() and takes the first 3 names
     - If any error occurs during the calculations, it returns an empty Flux
     */
    public Flux<String> question5B() {
        return Flux.range(100, 901)
                .doOnSubscribe(subscription -> log.info("Starting question5B: processing numbers 100 to 1000"))
                .flatMap(i -> calculatorService.calculate(BigDecimal.valueOf(i)))
                .collectList()
                .flatMapMany(list -> {
                    Flux<String> names = userService.findAllNames();
                    return names != null ? names.take(3) : Flux.empty();
                })
                .doOnError(error -> log.error("Question5B encountered an error during calculation: {}", error.getMessage()))
                .onErrorResume(e -> Flux.empty())
                .doOnComplete(() -> log.info("Finished question5B"));
    }

}
