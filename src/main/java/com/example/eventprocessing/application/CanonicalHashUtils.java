package com.example.eventprocessing.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigDecimal;
import java.util.*;

/**
 * Canonical, order-independent, type-preserving hash of the business fields of a request (spec 3.1).
 * Result format: "sha256:&lt;hex&gt;".
 */
public final class CanonicalHashUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CanonicalHashUtils() {
    }

    public static String calculateCanonicalHash(
            String customerId,
            String type,
            BigDecimal amount,
            String currency,
            String timestampStr,
            Map<String, Object> metadata) {

        try {
            Map<String, Object> canonicalMap = new TreeMap<>();
            canonicalMap.put("customerId", customerId != null ? customerId : "");
            canonicalMap.put("type", type != null ? type : "");
            canonicalMap.put("amount", amount != null ? normalizeNumber(amount) : "0");
            canonicalMap.put("currency", currency != null ? currency : "");
            canonicalMap.put("timestamp", timestampStr != null ? timestampStr : "");
            canonicalMap.put("metadata", canonicalizeMetadata(metadata));

            String canonicalJson = MAPPER.writeValueAsString(canonicalMap);
            return "sha256:" + DigestUtils.sha256Hex(canonicalJson);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to calculate canonical hash", e);
        }
    }

    private static String normalizeNumber(BigDecimal value) {
        BigDecimal stripped = value.signum() == 0 ? BigDecimal.ZERO : value.stripTrailingZeros();
        return stripped.toPlainString();
    }

    private static Map<String, Object> canonicalizeMetadata(Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            result.put(entry.getKey(), canonicalizeValue(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object canonicalizeValue(Object val) {
        if (val == null) {
            return "null:";
        } else if (val instanceof Map) {
            return canonicalizeMetadata((Map<String, Object>) val);
        } else if (val instanceof List<?> list) {
            List<Object> newList = new ArrayList<>(list.size());
            for (Object item : list) {
                newList.add(canonicalizeValue(item));
            }
            return newList;
        } else if (val instanceof Number) {
            // type-tagged so that the number 1 and the string "1" do not collide
            return "num:" + normalizeNumber(new BigDecimal(val.toString()));
        } else if (val instanceof Boolean) {
            return "bool:" + val;
        }
        return "str:" + val;
    }
}
