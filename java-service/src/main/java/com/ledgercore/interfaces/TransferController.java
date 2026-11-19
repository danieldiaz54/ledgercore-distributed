package com.ledgercore.interfaces;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ledgercore.application.TransferSagaOrchestrator;
import com.ledgercore.application.TransferSagaOrchestrator.AccountNotFoundException;
import com.ledgercore.application.TransferSagaOrchestrator.DistributedLockContentionException;
import com.ledgercore.application.TransferSagaOrchestrator.IdempotencyConflictException;
import com.ledgercore.application.TransferSagaOrchestrator.InsufficientFundsException;
import com.ledgercore.application.TransferSagaOrchestrator.InvalidTransferException;
import com.ledgercore.application.TransferSagaOrchestrator.SagaSnapshot;
import com.ledgercore.application.TransferSagaOrchestrator.TransferCommand;
import com.ledgercore.application.TransferSagaOrchestrator.TransferExecutionResult;
import com.ledgercore.domain.LedgerEntry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST Controller for LedgerCore Distributed Transfers (API v1).
 *
 * <p>Enforces Bean Validation DTO contracts, {@code Idempotency-Key} replay vs conflict semantics,
 * and maps all domain exceptions to semantic HTTP status codes without leaking stack traces.
 */
@RestController
@RequestMapping(path = "/api/v1/transfers", produces = MediaType.APPLICATION_JSON_VALUE)
public class TransferController {

    private final TransferSagaOrchestrator orchestrator;

    public TransferController(TransferSagaOrchestrator orchestrator) {
        this.orchestrator = Objects.requireNonNull(orchestrator, "orchestrator must not be null");
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TransferHttpResponse> createTransfer(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKeyHeader,
            @Valid @RequestBody CreateTransferHttpRequest body
    ) {
        String resolvedKey = (idempotencyKeyHeader != null && !idempotencyKeyHeader.isBlank())
                ? idempotencyKeyHeader.trim()
                : body.idempotencyKey();

        TransferCommand command = new TransferCommand(
                resolvedKey,
                body.sourceAccountId(),
                body.destinationAccountId(),
                body.amountCents(),
                body.currency() != null ? body.currency() : "USD",
                Boolean.TRUE.equals(body.simulateSettlementFailure())
        );

        TransferExecutionResult result = orchestrator.executeTransfer(command);
        TransferHttpResponse responseBody = TransferHttpResponse.from(result);

        HttpStatus status = result.idempotentReplay() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(responseBody);
    }

    // -------------------------------------------------------------------------
    // Semantic Exception -> HTTP Status Mapping (Zero Stack-Trace Leakage)
    // -------------------------------------------------------------------------

    @ExceptionHandler(InvalidTransferException.class)
    public ResponseEntity<ErrorEnvelope> handleInvalidTransfer(InvalidTransferException ex) {
        return errorResponse(
                HttpStatus.BAD_REQUEST,
                InvalidTransferException.CODE,
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                Map.of()
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorEnvelope> handleValidationFailure(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(err -> "Field '" + err.getField() + "' " + err.getDefaultMessage())
                .orElse("Request validation failed.");
        return errorResponse(
                HttpStatus.BAD_REQUEST,
                InvalidTransferException.CODE,
                "ValidationError",
                message,
                Map.of()
        );
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public ResponseEntity<ErrorEnvelope> handleAccountNotFound(AccountNotFoundException ex) {
        return errorResponse(
                HttpStatus.NOT_FOUND,
                AccountNotFoundException.CODE,
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                Map.of("account_id", ex.getResourceId())
        );
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorEnvelope> handleIdempotencyConflict(IdempotencyConflictException ex) {
        return errorResponse(
                HttpStatus.CONFLICT,
                IdempotencyConflictException.CODE,
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                Map.of("idempotency_key", ex.getIdempotencyKey())
        );
    }

    @ExceptionHandler(DistributedLockContentionException.class)
    public ResponseEntity<ErrorEnvelope> handleLockContention(DistributedLockContentionException ex) {
        return errorResponse(
                HttpStatus.CONFLICT,
                DistributedLockContentionException.CODE,
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                Map.of("resource_key", ex.getResourceKey())
        );
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public ResponseEntity<ErrorEnvelope> handleInsufficientFunds(InsufficientFundsException ex) {
        return errorResponse(
                HttpStatus.UNPROCESSABLE_ENTITY,
                InsufficientFundsException.CODE,
                ex.getClass().getSimpleName(),
                ex.getMessage(),
                Map.of(
                        "account_id", ex.getAccountId(),
                        "available_cents", ex.getAvailableCents(),
                        "requested_cents", ex.getRequestedCents()
                )
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorEnvelope> handleUnexpectedError(Exception ignored) {
        return errorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "ERR-500-INTERNAL-ERROR",
                "InternalServerError",
                "An unexpected internal server error occurred.",
                Map.of()
        );
    }

    private ResponseEntity<ErrorEnvelope> errorResponse(
            HttpStatus status,
            String code,
            String type,
            String message,
            Map<String, Object> details
    ) {
        return ResponseEntity.status(status)
                .body(new ErrorEnvelope(new ErrorDetail(code, type, message, details)));
    }

    // -------------------------------------------------------------------------
    // Typed REST Request / Response DTOs
    // -------------------------------------------------------------------------

    public record CreateTransferHttpRequest(
            @JsonProperty("idempotency_key")
            String idempotencyKey,

            @NotBlank
            @JsonProperty("source_account_id")
            String sourceAccountId,

            @NotBlank
            @JsonProperty("destination_account_id")
            String destinationAccountId,

            @Positive
            @JsonProperty("amount_cents")
            long amountCents,

            @Pattern(regexp = "^[A-Z]{3}$")
            @JsonProperty("currency")
            String currency,

            @JsonProperty("simulate_settlement_failure")
            Boolean simulateSettlementFailure
    ) {}

    public record LedgerEntryDto(
            @JsonProperty("entry_id") String entryId,
            @JsonProperty("transaction_id") String transactionId,
            @JsonProperty("account_id") String accountId,
            @JsonProperty("direction") String direction,
            @JsonProperty("amount_cents") long amountCents,
            @JsonProperty("currency") String currency,
            @JsonProperty("entry_type") String entryType,
            @JsonProperty("created_at") String createdAt
    ) {
        static LedgerEntryDto from(LedgerEntry entry) {
            return new LedgerEntryDto(
                    entry.getEntryId(),
                    entry.getTransactionId(),
                    entry.getAccountId(),
                    entry.getDirection().name(),
                    entry.getAmountCents(),
                    entry.getCurrency(),
                    entry.getEntryType(),
                    entry.getCreatedAt().toString()
            );
        }
    }

    public record TransferHttpResponse(
            @JsonProperty("idempotent_replay") boolean idempotentReplay,
            @JsonProperty("saga") SagaSnapshot saga,
            @JsonProperty("ledger_entries") List<LedgerEntryDto> ledgerEntries,
            @JsonProperty("outbox_events_relayed") int outboxEventsRelayed
    ) {
        static TransferHttpResponse from(TransferExecutionResult result) {
            return new TransferHttpResponse(
                    result.idempotentReplay(),
                    result.saga(),
                    result.ledgerEntries().stream().map(LedgerEntryDto::from).toList(),
                    result.outboxEventsRelayed()
            );
        }
    }

    public record ErrorDetail(
            String code,
            String type,
            String message,
            Map<String, Object> details
    ) {}

    public record ErrorEnvelope(ErrorDetail error) {}
}
