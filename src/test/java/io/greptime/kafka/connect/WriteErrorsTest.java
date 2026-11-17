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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    void retryHintTakesPrecedence() {
        // InvalidArguments would not be retried by code, but the server says it is retryable.
        assertTrue(WriteErrors.isRetryable(grpcError(Status.INVALID_ARGUMENT, "1004", "retryable")));
        assertTrue(WriteErrors.isRetryable(grpcError(Status.UNAVAILABLE, "4008", "retry_after_route_refresh")));
        // UNAVAILABLE would be retried by gRPC code, but the server says it is not.
        assertFalse(WriteErrors.isRetryable(grpcError(Status.UNAVAILABLE, "4008", "non_retryable")));
    }

    @Test
    void statusCodeIsUsedWithoutHint() {
        assertTrue(WriteErrors.isRetryable(grpcError(Status.RESOURCE_EXHAUSTED, "4009", null))); // RegionBusy
        assertTrue(WriteErrors.isRetryable(grpcError(Status.RESOURCE_EXHAUSTED, "6001", null))); // RateLimited
        assertFalse(WriteErrors.isRetryable(grpcError(Status.INVALID_ARGUMENT, "1004", null))); // InvalidArguments
        assertFalse(WriteErrors.isRetryable(grpcError(Status.UNAUTHENTICATED, "7002", null))); // UserPasswordMismatch
    }

    @Test
    void grpcCodeIsUsedWithoutGreptimeMetadata() {
        assertTrue(WriteErrors.isRetryable(Status.UNAVAILABLE.asRuntimeException()));
        assertTrue(WriteErrors.isRetryable(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        assertFalse(WriteErrors.isRetryable(Status.INVALID_ARGUMENT.asRuntimeException()));
        assertFalse(WriteErrors.isRetryable(new IllegalArgumentException("bad value")));
    }

    @Test
    void connectFailureIsRetryableThroughWrappers() {
        assertTrue(WriteErrors.isRetryable(new ExecutionException(new ConnectFailException("down"))));
    }
}
