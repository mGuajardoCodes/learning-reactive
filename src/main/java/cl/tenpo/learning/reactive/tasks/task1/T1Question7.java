package cl.tenpo.learning.reactive.tasks.task1;


import cl.tenpo.learning.reactive.utils.model.UserAccount;
import cl.tenpo.learning.reactive.utils.service.AccountService;
import cl.tenpo.learning.reactive.utils.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class T1Question7 {

    private final UserService userService;
    private final AccountService accountService;

    public Mono<UserAccount> question7(String userId) {
        return Mono.zip(
                        userService.getUserById(userId)
                                .doOnSubscribe(s -> log.info("Fetching user with id: {}", userId))
                                .switchIfEmpty(Mono.defer(() ->
                                        Mono.error(new RuntimeException("User not found for userId: " + userId)))),
                        accountService.getAccountByUserId(userId)
                                .doOnSubscribe(s -> log.info("Fetching account for user id: {}", userId))
                                .switchIfEmpty(Mono.defer(() ->
                                        Mono.error(new RuntimeException("User account not found for userId: " + userId))))
                )
                .map(tuple -> new UserAccount(tuple.getT1(), tuple.getT2()))
                .doOnSuccess(userAccount -> log.info("Successfully retrieved UserAccount: {}", userAccount))
                .doOnError(error -> log.error("Error retrieving UserAccount for user id {}: {}", userId,
                        error.getMessage()));
    }
}
