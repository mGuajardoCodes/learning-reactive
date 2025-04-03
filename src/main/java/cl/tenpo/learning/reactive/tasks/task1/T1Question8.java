package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.exception.AuthorizationTimeoutException;
import cl.tenpo.learning.reactive.utils.exception.PaymentProcessingException;
import cl.tenpo.learning.reactive.utils.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class T1Question8 {

    private final TransactionService transactionService;

    private static final int TRX_ID = 11111;
    private static final int DURATION = 3;
    private static final int ATTEMPTS = 3;
    private static final int DURATION_OF_MILLIS = 500;

    public Mono<String> question8() {
        return transactionService.authorizeTransaction(TRX_ID)
                .doOnSubscribe(sub -> log.info("Starting question8: authorizing transactionId {}", TRX_ID))
                .timeout(Duration.ofSeconds(DURATION), Mono.error(new AuthorizationTimeoutException("Timeout")))
                .retryWhen(
                        Retry.fixedDelay(ATTEMPTS, Duration.ofMillis(DURATION_OF_MILLIS))
                                .filter(throwable -> !(throwable instanceof AuthorizationTimeoutException))
                )
                .onErrorMap(throwable ->
                        throwable instanceof AuthorizationTimeoutException
                                ? throwable
                                : new PaymentProcessingException(throwable.getMessage(), throwable)
                )
                .doOnError(error -> log.error("Question8 failed with error: {}", error.getMessage()))
                .doOnSuccess(result -> log.info("Finished question8 with result: {}", result));
    }
}
