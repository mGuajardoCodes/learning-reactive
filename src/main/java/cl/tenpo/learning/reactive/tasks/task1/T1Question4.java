package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.service.CountryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class T1Question4 {

    private final CountryService countryService;

    public Flux<String> question4A() {
        // Consume the flux only once, limiting it to 200 elements
        Flux<String> sharedCountries = countryService.findAllCountries()
                .doOnSubscribe(subscription -> log.info("Starting question4A: consuming 200 countries"))
                .take(200)
                .cache();
        // reminder :p
        // cache(): Stores the 200-country batch in memory so that the expensive findAllCountries method is only executed once

        // Calculate the frequency map asynchronously and log it to the console
        sharedCountries
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .doOnError(error -> log.error("Error while grouping countries: {}", error.getMessage()))
                .subscribe(countMap -> log.info("Country count: {}", countMap));

        // Return the list of unique countries sorted alphabetically
        return sharedCountries
                .distinct()
                .collectSortedList()
                .flatMapMany(Flux::fromIterable)
                .doOnError(error -> log.error("Error in question4A result processing: {}", error.getMessage()))
                .doOnComplete(() -> log.info("Finished question4A with sorted unique countries"));
    }

}
