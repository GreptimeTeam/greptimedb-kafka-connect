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
 * Decides whether a failed write may succeed if retried.
 *
 * <p>GreptimeDB reports write errors as gRPC errors with the status code and, since v1.2, a retry hint in
 * the trailers. The ingester does not inspect them, so its own retry never applies to server errors.
 */
final class WriteErrors {

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

    static boolean isRetryable(Throwable error) {
        Throwable e = unwrap(error);
        if (e instanceof ConnectFailException) {
            return true;
        }
        io.grpc.Status grpcStatus = io.grpc.Status.fromThrowable(e);
        Metadata trailers = io.grpc.Status.trailersFromThrowable(e);
        if (trailers != null) {
            String hint = trailers.get(RETRY_HINT_KEY);
            if (hint != null) {
                return !HINT_NON_RETRYABLE.equals(hint);
            }
            String code = trailers.get(ERR_CODE_KEY);
            if (code != null) {
                try {
                    return isRetryableStatusCode(Integer.parseInt(code.trim()));
                } catch (NumberFormatException ignored) {
                    // Fall through to the gRPC status code.
                }
            }
        }
        return RETRYABLE_GRPC_CODES.contains(grpcStatus.getCode());
    }

    /** For errors returned in the response header rather than as a gRPC error. */
    static boolean isRetryableStatusCode(int code) {
        if (code == DEADLINE_EXCEEDED_CODE) {
            return true;
        }
        Status status = Status.parse(code);
        return status != null && RETRYABLE_STATUS.contains(status);
    }

    private static Throwable unwrap(Throwable e) {
        while ((e instanceof ExecutionException || e instanceof CompletionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    private WriteErrors() {}
}
