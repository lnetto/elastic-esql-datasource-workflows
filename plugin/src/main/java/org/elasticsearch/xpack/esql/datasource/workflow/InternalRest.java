/*
 * Kibana Workflows connector for ES|QL Data Federation.
 */
package org.elasticsearch.xpack.esql.datasource.workflow;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs a REST request through this node's own {@link RestController}, in memory, on the current thread
 * context. That context already holds the querying user's authentication (ES|QL runs the query in it),
 * so the API is authorised exactly as if the user had called it: no network hop, no credentials.
 */
final class InternalRest {

    record Response(int status, String body) {}

    private InternalRest() {}

    static Response call(RestRequest.Method method, String uri, String body, Duration timeout) {
        RestController controller = WorkflowNode.restController();
        ThreadContext threadContext = WorkflowNode.threadContext();
        MemRequest http = new MemRequest(method, uri, body);
        RestRequest request = RestRequest.request(XContentParserConfiguration.EMPTY.withRegistry(WorkflowNode.xContentRegistry()), http, new MemChannel());
        CompletableFuture<RestResponse> done = new CompletableFuture<>();
        AbstractRestChannel channel = new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {
                done.complete(response);
            }
        };
        // A fresh context, as on an HTTP worker thread, that carries over only who the caller is:
        // dispatch sets its own per-request headers and refuses ones that are already there.
        String authentication = threadContext.getHeader(WorkflowNode.AUTHENTICATION_HEADER);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            if (authentication != null) {
                threadContext.putHeader(WorkflowNode.AUTHENTICATION_HEADER, authentication);
            }
            controller.dispatchRequest(request, channel, threadContext);
        }
        RestResponse response;
        try {
            response = done.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException(method + " " + uri + " didn't answer within " + timeout.toSeconds() + "s");
        } catch (ExecutionException e) {
            throw new IllegalStateException(method + " " + uri + " failed: " + e.getCause(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted calling " + uri, e);
        }
        return new Response(response.status().getStatus(), bodyOf(response, timeout));
    }

    private static String bodyOf(RestResponse response, Duration timeout) {
        try {
            if (response.isChunked() == false) {
                BytesReference content = response.content();
                return content == null ? "" : content.utf8ToString();
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ChunkedRestResponseBodyPart part = response.chunkedContent();
            while (true) {
                while (part.isPartComplete() == false) {
                    try (ReleasableBytesReference chunk = part.encodeChunk(1 << 16, BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
                        chunk.writeTo(out);
                    }
                }
                if (part.isLastPart()) {
                    break;
                }
                CompletableFuture<ChunkedRestResponseBodyPart> next = new CompletableFuture<>();
                part.getNextPart(ActionListener.wrap(next::complete, next::completeExceptionally));
                part = next.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("reading the API response failed: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted reading the API response", e);
        }
    }

    /** A request as it would arrive over HTTP, minus the network. */
    private static final class MemRequest implements HttpRequest {
        private final RestRequest.Method method;
        private final String uri;
        private final Map<String, List<String>> headers = new HashMap<>();
        private HttpBody body;

        MemRequest(RestRequest.Method method, String uri, String body) {
            this.method = method;
            this.uri = uri;
            this.headers.put("Accept", List.of("application/json"));
            if (body != null) {
                this.headers.put("Content-Type", List.of("application/json"));
                this.body = HttpBody.fromBytesReference(new BytesArray(body.getBytes(StandardCharsets.UTF_8)));
            } else {
                this.body = HttpBody.empty();
            }
        }

        @Override
        public RestRequest.Method method() {
            return method;
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public HttpBody body() {
            return body;
        }

        @Override
        public void setBody(HttpBody body) {
            this.body = body;
        }

        @Override
        public List<String> strictCookies() {
            return List.of();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            headers.remove(header);
            return this;
        }

        @Override
        public boolean hasContent() {
            return body instanceof HttpBody.Full full && full.bytes().length() > 0;
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return new MemResponse();
        }

        @Override
        public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
            return new MemResponse();
        }

        @Override
        public Exception getInboundException() {
            return null;
        }

        @Override
        public void release() {}
    }

    private static final class MemResponse implements HttpResponse {
        @Override
        public void addHeader(String name, String value) {}

        @Override
        public boolean containsHeader(String name) {
            return false;
        }
    }

    private static final class MemChannel implements HttpChannel {
        private static final InetSocketAddress LOOPBACK = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return LOOPBACK;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return LOOPBACK;
        }

        @Override
        public void close() {}

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public boolean isOpen() {
            return true;
        }
    }
}
