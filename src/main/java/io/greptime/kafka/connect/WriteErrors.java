/*
 * Copyright 2026 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.kafka.connect;

import io.greptime.Status;
import io.greptime.rpc.errors.ConnectFailException;
import io.grpc.Metadata;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Classifies failed writes.
 *
 * <p>GreptimeDB reports write errors as gRPC errors with the status code and, since v1.2, a retry hint in
 * the trailers. The ingester does not inspect them, so its own retry never applies to server errors.
 */
final class WriteErrors {

    enum Kind {
        /** May succeed if the same request is sent again. */
        RETRYABLE,
        /** The request carries rows the server rejects, e.g. a value that does not match the column type. */
        DATA,
        /** Neither retrying nor dropping rows helps, e.g. an authentication failure. */
        FATAL
    }

    static final Metadata.Key<String> ERR_CODE_KEY =
            Metadata.Key.of("x-greptime-err-code", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> RETRY_HINT_KEY =
            Metadata.Key.of("x-greptime-err-retry-hint", Metadata.ASCII_STRING_MARSHALLER);

    private static final String HINT_NON_RETRYABLE = "non_retryable";

    /**
     * Used for servers that do not send a retry hint: StatusCode::is_retryable of GreptimeDB before v1.2,
     * plus RateLimited and DeadlineExceeded, which are transient for a sink that replays the same rows.
     */
    private static final Set<Status> RETRYABLE_STATUS = EnumSet.of(
            Status.Internal,
            Status.RegionNotReady,
            Status.RegionBusy,
            Status.TableUnavailable,
            Status.StorageUnavailable,
            Status.RuntimeResourcesExhausted,
            Status.RateLimited);

    /** Used when the error carries no GreptimeDB status, e.g. it came from the transport or a proxy. */
    private static final Set<io.grpc.Status.Code> RETRYABLE_GRPC_CODES = EnumSet.of(
            io.grpc.Status.Code.UNAVAILABLE,
            io.grpc.Status.Code.DEADLINE_EXCEEDED,
            io.grpc.Status.Code.RESOURCE_EXHAUSTED);

    // GreptimeDB StatusCode::DeadlineExceeded, not present in the ingester's Status enum.
    private static final int DEADLINE_EXCEEDED_CODE = 1008;

    static Kind classify(Throwable error) {
        Throwable e = unwrap(error);
        if (e instanceof ConnectFailException) {
            return Kind.RETRYABLE;
        }
        io.grpc.Status.Code grpcCode = io.grpc.Status.fromThrowable(e).getCode();
        Metadata trailers = io.grpc.Status.trailersFromThrowable(e);
        Integer statusCode = trailers == null ? null : parseStatusCode(trailers.get(ERR_CODE_KEY));
        String hint = trailers == null ? null : trailers.get(RETRY_HINT_KEY);
        if (hint != null && !HINT_NON_RETRYABLE.equals(hint)) {
            return Kind.RETRYABLE;
        }
        if (statusCode != null) {
            Kind kind = classifyStatusCode(statusCode);
            // The hint overrides the status code's retryability, not its meaning.
            return hint != null && kind == Kind.RETRYABLE ? Kind.FATAL : kind;
        }
        if (hint == null && RETRYABLE_GRPC_CODES.contains(grpcCode)) {
            return Kind.RETRYABLE;
        }
        // Only a non-gRPC error has code UNKNOWN here, and client-side failures are not data errors.
        return grpcCode == io.grpc.Status.Code.INVALID_ARGUMENT ? Kind.DATA : Kind.FATAL;
    }

    /** For a GreptimeDB status code, also used for errors returned in the response header. */
    static Kind classifyStatusCode(int code) {
        if (code == DEADLINE_EXCEEDED_CODE) {
            return Kind.RETRYABLE;
        }
        Status status = Status.parse(code);
        if (status == Status.InvalidArguments) {
            return Kind.DATA;
        }
        return status != null && RETRYABLE_STATUS.contains(status) ? Kind.RETRYABLE : Kind.FATAL;
    }

    private static Integer parseStatusCode(String code) {
        if (code == null) {
            return null;
        }
        try {
            return Integer.parseInt(code.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Throwable unwrap(Throwable e) {
        while ((e instanceof ExecutionException || e instanceof CompletionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    private WriteErrors() {}
}
