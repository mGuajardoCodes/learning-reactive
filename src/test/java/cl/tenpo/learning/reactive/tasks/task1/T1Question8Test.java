package cl.tenpo.learning.reactive.tasks.task1;

import cl.tenpo.learning.reactive.utils.exception.AuthorizationTimeoutException;
import cl.tenpo.learning.reactive.utils.exception.PaymentProcessingException;
import cl.tenpo.learning.reactive.utils.service.TransactionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class T1Question8Test {

    @InjectMocks
    private T1Question8 t1Question8;

    @Mock
    private TransactionService transactionServiceMock;

    @Test
    @DisplayName("PREGUNTA 8 - Transacción autorizada")
    void question8_uc1_test() {

        when(transactionServiceMock.authorizeTransaction(11111))
                .thenReturn(Mono.just("Autorizado"));

        Mono<String> outputMono = t1Question8.question8();

        StepVerifier.create(outputMono.log())
                .expectNext("Autorizado")
                .verifyComplete();

        verify(transactionServiceMock, times(1)).authorizeTransaction(11111);
        verifyNoMoreInteractions(transactionServiceMock);
    }

    @Test
    @DisplayName("PREGUNTA 8 - Transacción autorizada - 1 retry")
    void question8_uc2_test() {

        AtomicInteger counter = new AtomicInteger(0);

        Mono<String> mockAuthorization = Mono.defer(() -> Mono.just(counter.getAndIncrement())
                .filter(cnt -> cnt != 0)
                .map(__ -> "Autorizado")
                .switchIfEmpty(Mono.error(() -> new RuntimeException("Fallo inicial"))));

        when(transactionServiceMock.authorizeTransaction(11111))
                .thenReturn(mockAuthorization);

        Mono<String> outputMono = t1Question8.question8();

        StepVerifier.create(outputMono.log())
                .expectNext("Autorizado")
                .verifyComplete();

        verify(transactionServiceMock, times(1)).authorizeTransaction(11111);
        verifyNoMoreInteractions(transactionServiceMock);
    }

    @Test
    @DisplayName("PREGUNTA 8 - Transacción autorizada - service timeout & AuthorizationTimeoutException")
    void question8_uc3_test() {

        when(transactionServiceMock.authorizeTransaction(11111)).thenReturn(Mono.never());

        Mono<String> outputMono = t1Question8.question8();

        StepVerifier.create(outputMono.log())
                .expectError(AuthorizationTimeoutException.class)
                .verify();

        verify(transactionServiceMock, times(1)).authorizeTransaction(11111);
        verifyNoMoreInteractions(transactionServiceMock);
    }

    @Test
    @DisplayName("PREGUNTA 8 - Transacción autorizada - 3 retries & PaymentProcessingException")
    void question8_uc4_test() {

        when(transactionServiceMock.authorizeTransaction(11111)).thenReturn(Mono.error(new RuntimeException("Falla crítica")));

        Mono<String> outputMono = t1Question8.question8();

        StepVerifier.create(outputMono.log())
                .expectError(PaymentProcessingException.class)
                .verify();

        verify(transactionServiceMock, times(1)).authorizeTransaction(11111);
        verifyNoMoreInteractions(transactionServiceMock);
    }
}
