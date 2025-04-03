package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.model.Page;
import cl.tenpo.learning.reactive.utils.service.CountryService;
import cl.tenpo.learning.reactive.utils.service.TranslatorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class T1Question3 {

    private final CountryService countryService;
    private final TranslatorService translatorService;

    public Flux<String> question3A(final Page<String> page) {
        return Mono.justOrEmpty(page)
                .doOnSubscribe(subscription -> log.info("Starting question3A for page: {}", page))
                .flatMapMany(p ->
                        Flux.fromIterable(p.items())
                )
                .doOnError(error -> log.error("Question3A failed with error: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question3A"));
    }

    public Flux<String> question3B(final String country) {
        return countryService.findCurrenciesByCountry(country)
                .doOnSubscribe(subscription -> log.info("Starting question3B for country: {}", country))
                .distinct()
                .doOnError(error -> log.error("Question3B failed with error: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question3B"));
    }

    public Flux<String> question3C() {
        return countryService.findAllCountries()
                .doOnSubscribe(subscription -> log.info("Starting question3C"))
                .take(3)
                .flatMap(country -> Mono.justOrEmpty(translatorService.translate(country)))
                .doOnError(error -> log.error("Question3C failed with error: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question3C"));
    }

}
