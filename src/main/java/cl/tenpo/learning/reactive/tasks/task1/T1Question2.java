package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.service.CountryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Slf4j
@Component
@RequiredArgsConstructor
public class T1Question2 {

    private final CountryService countryService;

    private static final String COUNTRY_TO_FILTER_2B = "Argentina";
    private static final String COUNTRY_TO_FILTER_2C = "France";

    public Flux<String> question2A() {
        return countryService.findAllCountries()
                .doOnSubscribe(subscription -> log.info("Starting question2A"))
                .distinct()
                .take(5)
                .doOnError(error -> log.error("Question2A failed with error: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question2A"));
    }

    public Flux<String> question2B() {
        return countryService.findAllCountries()
                .doOnSubscribe(subscription -> log.info("Starting question2B"))
                .takeUntil(COUNTRY_TO_FILTER_2B::equalsIgnoreCase)
                .doOnError(error -> log.error("Question2B failed with error: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question2B"));
    }

    public Flux<String> question2C() {
        return countryService.findAllCountries()
                .doOnSubscribe(subscription -> log.info("Starting question2C"))
                .takeWhile(country -> !COUNTRY_TO_FILTER_2C.equalsIgnoreCase(country))
                .doOnError(error -> log.error("Question2C failed with error: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question2C"));
    }

}
