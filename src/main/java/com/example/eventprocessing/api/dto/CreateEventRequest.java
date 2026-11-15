package com.example.eventprocessing.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.*;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Currency;
import java.util.Map;

@Data
public class CreateEventRequest {

    public static final int MAX_METADATA_KEYS = 20;
    public static final int MAX_METADATA_KEY_LENGTH = 50;
    public static final int MAX_METADATA_VALUE_LENGTH = 500;
    public static final int MAX_METADATA_TOTAL_CHARS = 4000;
    /** Events from the "future" beyond this tolerance are rejected (clock-skew allowance). */
    public static final Duration MAX_FUTURE_SKEW = Duration.ofDays(1);

    @NotBlank(message = "eventId is required")
    @Size(max = 100, message = "eventId must not exceed 100 characters")
    private String eventId;

    @NotBlank(message = "customerId is required")
    @Size(max = 100, message = "customerId must not exceed 100 characters")
    private String customerId;

    @NotBlank(message = "type is required")
    @Pattern(regexp = "^[A-Z0-9_]{1,50}$", message = "type must be alphanumeric uppercase (1-50 chars)")
    private String type;

    @NotNull(message = "amount is required")
    @Positive(message = "amount must be positive")
    @DecimalMax(value = "1000000000000", message = "amount exceeds maximum allowed value")
    @Digits(integer = 13, fraction = 4, message = "amount must have at most 13 integer digits and 4 fraction digits")
    private BigDecimal amount;

    @NotBlank(message = "currency is required")
    @Pattern(regexp = "^[A-Z]{3}$", message = "currency must be a 3-letter ISO code")
    private String currency;

    @NotNull(message = "timestamp is required")
    private Instant timestamp;

    private Map<String, Object> metadata;

    @JsonIgnore
    @AssertTrue(message = "currency must be a valid ISO 4217 code")
    public boolean isCurrencyValid() {
        if (currency == null || !currency.matches("^[A-Z]{3}$")) {
            return true; // reported by the @Pattern/@NotBlank constraints
        }
        try {
            Currency.getInstance(currency);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @JsonIgnore
    @AssertTrue(message = "timestamp must not be more than 24 hours in the future")
    public boolean isTimestampValid() {
        return timestamp == null || !timestamp.isAfter(Instant.now().plus(MAX_FUTURE_SKEW));
    }

    @JsonIgnore
    @AssertTrue(message = "metadata structure invalid: max 20 keys, key 1-50 chars without '.' or leading '$', "
            + "values must be scalar (string up to 500 chars, number, boolean), total size up to 4000 chars")
    public boolean isMetadataValid() {
        if (metadata == null) {
            return true;
        }
        if (metadata.size() > MAX_METADATA_KEYS) {
            return false;
        }
        int total = 0;
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isEmpty() || key.length() > MAX_METADATA_KEY_LENGTH
                    || key.contains(".") || key.startsWith("$")) {
                return false;
            }
            total += key.length();
            Object value = entry.getValue();
            if (value == null || value instanceof Number || value instanceof Boolean) {
                continue;
            }
            if (value instanceof String s) {
                if (s.length() > MAX_METADATA_VALUE_LENGTH) {
                    return false;
                }
                total += s.length();
            } else {
                return false; // nested objects / arrays are not allowed
            }
        }
        return total <= MAX_METADATA_TOTAL_CHARS;
    }
}
