package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.exception.ResourceNotFoundException;
import cl.tenpo.learning.reactive.utils.exception.UserServiceException;
import cl.tenpo.learning.reactive.utils.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class T1Question1 {

    private final UserService userService;

    private static final String RULE_OF_NAME = "A";
    private static final Integer DEFAULT_LENGTH = -1;

    public Mono<Integer> question1A() {
        return userService.findFirstName()
                .doOnSubscribe(subscription -> log.info("Starting question1A"))
                .filter(name -> name.startsWith(RULE_OF_NAME))
                .map(String::length)
                .defaultIfEmpty(DEFAULT_LENGTH)
                .doOnSuccess(result -> log.info("Finished question1A with result: {}", result));
    }

    public Mono<String> question1B() {
        return userService.findFirstName()
                .doOnSubscribe(subscription -> log.info("Starting question1B"))
                .flatMap(name -> userService.existByName(name)
                        .filter(exists -> exists)
                        .flatMap(exists -> userService.update(name))
                        .switchIfEmpty(Mono.defer(() -> userService.insert(name)))
                )
                .doOnSuccess(result -> log.info("Finished question1B with result: {}", result));
    }

    public Mono<String> question1C(final String name) {
        return Mono.just(name)
                .doOnSubscribe(subscription -> log.info("Starting question1C for name: {}", name))
                .flatMap(userService::findFirstByName)
                .switchIfEmpty(Mono.defer(() -> Mono.error(new ResourceNotFoundException())))
                .onErrorMap(error -> error instanceof ResourceNotFoundException ?
                        error :
                        new UserServiceException())
                .doOnError(error -> log.error("Question1C failed with error {}", error.getMessage()))
                .doOnSuccess(result -> log.info("Finished question1C with result: {}", result));
    }

}
