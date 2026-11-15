package com.example.eventprocessing.unit;

import com.example.eventprocessing.application.CanonicalHashUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CanonicalHashUtilsTest {

    private String hash(BigDecimal amount, Map<String, Object> metadata) {
        return CanonicalHashUtils.calculateCanonicalHash("c1", "PAYMENT", amount, "ILS", "2026-11-12T12:00:00Z", metadata);
    }

    @Test
    void hashHasSha256Prefix() {
        assertTrue(hash(new BigDecimal("1"), null).startsWith("sha256:"));
    }

    @Test
    void metadataKeyOrderDoesNotMatter() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("source", "mobile");
        a.put("channel", "app");
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("channel", "app");
        b.put("source", "mobile");
        assertEquals(hash(new BigDecimal("10"), a), hash(new BigDecimal("10"), b));
    }

    @Test
    void amountScaleDoesNotMatter() {
        assertEquals(hash(new BigDecimal("1250.50"), null), hash(new BigDecimal("1250.5"), null));
        assertEquals(hash(new BigDecimal("100"), null), hash(new BigDecimal("100.00"), null));
    }

    @Test
    void differentAmountChangesHash() {
        assertNotEquals(hash(new BigDecimal("1"), null), hash(new BigDecimal("2"), null));
    }

    @Test
    void numberAndStringInMetadataDoNotCollide() {
        assertNotEquals(hash(BigDecimal.ONE, Map.of("k", 1)), hash(BigDecimal.ONE, Map.of("k", "1")));
        assertNotEquals(hash(BigDecimal.ONE, Map.of("k", true)), hash(BigDecimal.ONE, Map.of("k", "true")));
    }

    @Test
    void nullAndEmptyMetadataAreEqual() {
        assertEquals(hash(BigDecimal.ONE, null), hash(BigDecimal.ONE, Map.of()));
    }
}
