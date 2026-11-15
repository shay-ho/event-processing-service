package com.example.eventprocessing.unit;

import com.example.eventprocessing.api.RequestSizeLimitFilter;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Spec section 12: payload size validation (413 for bodies above events.api.max-payload-bytes). */
class RequestSizeLimitFilterTest {

    private static final int LIMIT = 100;
    private final RequestSizeLimitFilter filter = new RequestSizeLimitFilter(LIMIT);

    private static MockHttpServletRequest post(String uri, byte[] body, boolean unknownLength) {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", uri) {
            @Override
            public long getContentLengthLong() {
                return unknownLength ? -1 : super.getContentLengthLong();
            }
        };
        req.setRequestURI(uri);
        req.setContent(body);
        req.setContentType("application/json");
        return req;
    }

    private static byte[] bytes(int n) {
        return "x".repeat(n).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void declaredContentLengthAboveLimitIsRejectedWith413AndBodyIsNotRead() throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(post("/api/events", bytes(LIMIT + 1), false), res, chain);

        assertEquals(413, res.getStatus());
        assertTrue(res.getContentAsString().contains("Payload Too Large"));
        assertNull(chain.getRequest(), "controller must not be invoked");
    }

    @Test
    void bodyWithinLimitPassesThrough() throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(post("/api/events", bytes(LIMIT), false), res, chain);

        assertEquals(200, res.getStatus());
        assertNotNull(chain.getRequest());
    }

    @Test
    void chunkedBodyAboveLimitIsRejectedWithoutDeclaredLength() throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(post("/api/events", bytes(LIMIT * 50), true), res, chain);

        assertEquals(413, res.getStatus());
        assertNull(chain.getRequest());
    }

    @Test
    void chunkedBodyWithinLimitIsReplayedToTheController() throws Exception {
        byte[] body = bytes(40);
        MockHttpServletResponse res = new MockHttpServletResponse();
        AtomicReference<byte[]> seen = new AtomicReference<>();
        filter.doFilter(post("/api/events", body, true), res, (request, response) ->
                seen.set(request.getInputStream().readAllBytes()));

        assertEquals(200, res.getStatus());
        assertArrayEquals(body, seen.get());
    }

    @Test
    void otherPathsAndMethodsAreNotLimited() throws Exception {
        MockHttpServletResponse res = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(post("/actuator/health", bytes(LIMIT * 10), false), res, chain);
        assertNotNull(chain.getRequest());

        MockHttpServletRequest get = new MockHttpServletRequest("GET", "/api/events/abc");
        get.setRequestURI("/api/events/abc");
        MockFilterChain chain2 = new MockFilterChain();
        filter.doFilter(get, new MockHttpServletResponse(), chain2);
        assertNotNull(chain2.getRequest());
    }

    @Test
    void nonPositiveLimitIsRejectedAtStartup() {
        assertThrows(IllegalArgumentException.class, () -> new RequestSizeLimitFilter(0));
    }
}
