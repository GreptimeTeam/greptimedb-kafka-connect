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

import static io.greptime.kafka.connect.WriteErrors.classify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import io.greptime.kafka.connect.WriteErrors.Kind;
import io.greptime.rpc.errors.ConnectFailException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class WriteErrorsTest {

    private static StatusRuntimeException grpcError(Status status, String errCode, String hint) {
        Metadata trailers = new Metadata();
        if (errCode != null) {
            trailers.put(WriteErrors.ERR_CODE_KEY, errCode);
        }
        if (hint != null) {
            trailers.put(WriteErrors.RETRY_HINT_KEY, hint);
        }
        return status.asRuntimeException(trailers);
    }

    @Test
    void retryHintDecidesRetryability() {
        assertEquals(Kind.RETRYABLE, classify(grpcError(Status.INVALID_ARGUMENT, "1004", "retryable")));
        assertEquals(Kind.RETRYABLE, classify(grpcError(Status.UNAVAILABLE, "4008", "retry_after_route_refresh")));
        // RegionNotReady would be retried by status code, but the server says it is not retryable.
        assertEquals(Kind.FATAL, classify(grpcError(Status.UNAVAILABLE, "4008", "non_retryable")));
        // What GreptimeDB v1.3 returns for a value that does not match the column type.
        assertEquals(Kind.DATA, classify(grpcError(Status.INVALID_ARGUMENT, "1004", "non_retryable")));
    }

    @Test
    void statusCodeIsUsedWithoutHint() {
        assertEquals(Kind.RETRYABLE, classify(grpcError(Status.RESOURCE_EXHAUSTED, "4009", null))); // RegionBusy
        assertEquals(Kind.RETRYABLE, classify(grpcError(Status.RESOURCE_EXHAUSTED, "6001", null))); // RateLimited
        assertEquals(Kind.DATA, classify(grpcError(Status.INVALID_ARGUMENT, "1004", null))); // InvalidArguments
        // Rows are not to blame for an authentication failure, so they must not be dropped.
        assertEquals(Kind.FATAL, classify(grpcError(Status.UNAUTHENTICATED, "7002", null)));
        assertEquals(Kind.FATAL, classify(grpcError(Status.NOT_FOUND, "4004", null))); // DatabaseNotFound
    }

    @Test
    void grpcCodeIsUsedWithoutGreptimeMetadata() {
        assertEquals(Kind.RETRYABLE, classify(Status.UNAVAILABLE.asRuntimeException()));
        assertEquals(Kind.RETRYABLE, classify(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        assertEquals(Kind.DATA, classify(Status.INVALID_ARGUMENT.asRuntimeException()));
        assertEquals(Kind.FATAL, classify(Status.PERMISSION_DENIED.asRuntimeException()));
        assertEquals(Kind.FATAL, classify(new IllegalArgumentException("bad value")));
    }

    @Test
    void connectFailureIsRetryableThroughWrappers() {
        assertEquals(Kind.RETRYABLE, classify(new ExecutionException(new ConnectFailException("down"))));
    }
}
