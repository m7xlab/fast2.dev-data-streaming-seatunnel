/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;

import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.OutOfRangeException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.Status;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigQuerySinkBatchWriterTest {

    @Test
    void testAlreadyExistsResponseAdvancesOffsetWithoutRecreatingStream() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFuture(errorResponse(Code.ALREADY_EXISTS)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.flush();

        assertEquals(1, streamWriter.appendCount);
        assertEquals(2, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(0, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testAlreadyExistsExceptionAdvancesOffsetWithoutRecreatingStream() {
        StatusCode statusCode = statusCode(StatusCode.Code.ALREADY_EXISTS);
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFailedFuture(
                                new AlreadyExistsException(
                                        new IOException("append response was lost"),
                                        statusCode,
                                        false)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.flush();

        assertEquals(1, streamWriter.appendCount);
        assertEquals(2, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(0, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testOffsetAlreadyExistsSkipsRowsTheStreamAlreadyHolds() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFailedFuture(offsetAlreadyExists(10, 0)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.flush();

        assertEquals(1, streamWriter.appendCount);
        assertEquals(2, streamWriter.successfulRowCount);
        assertEquals(0, sinkWriter.buffer.length());
    }

    @Test
    void testOffsetAlreadyExistsAppendsOnlyTheRowsMissingFromTheStream() {
        SequencedBigQueryWriter streamWriter =
                new SequencedBigQueryWriter(
                        ApiFutures.immediateFailedFuture(offsetAlreadyExists(2, 0)),
                        ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");
        sinkWriter.buffer.put("row-3");

        sinkWriter.flush();

        assertEquals(2, streamWriter.appended.size());
        assertEquals(3, streamWriter.appended.get(0).length());
        assertEquals(1, streamWriter.appended.get(1).length());
        assertEquals("row-3", streamWriter.appended.get(1).get(0));
        assertEquals(3, streamWriter.successfulRowCount);
        assertEquals(0, sinkWriter.buffer.length());
    }

    @Test
    void testOutOfRangeResponseFailsAndRetainsBuffer() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFuture(errorResponse(Code.OUT_OF_RANGE)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        assertThrows(BigQueryConnectorException.class, sinkWriter::flush);

        assertEquals(1, streamWriter.appendCount);
        assertEquals(0, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(2, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testOutOfRangeExceptionFailsAndRetainsBuffer() {
        StatusCode statusCode = statusCode(StatusCode.Code.OUT_OF_RANGE);
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFailedFuture(
                                new OutOfRangeException(
                                        new IOException("offset is beyond the stream end"),
                                        statusCode,
                                        false)));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row");

        assertThrows(BigQueryConnectorException.class, sinkWriter::flush);

        assertEquals(1, streamWriter.appendCount);
        assertEquals(0, streamWriter.successfulRowCount);
        assertEquals(0, streamWriter.closeCount);
        assertEquals(1, sinkWriter.buffer.length());
        assertSame(streamWriter, sinkWriter.streamWriter);
    }

    @Test
    void testCloseDoesNotFlushUncheckpointedRowsInBatchMode() {
        TestingBigQueryWriter streamWriter =
                new TestingBigQueryWriter(
                        ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()));
        BigQuerySinkBatchWriter sinkWriter = createSinkWriter(streamWriter);
        sinkWriter.buffer.put("row-1");
        sinkWriter.buffer.put("row-2");

        sinkWriter.close();

        // In batch mode (flushOnClose == false), close() must NOT append un-checkpointed rows
        // to preserve 2PC stream offset consistency during state recovery.
        assertEquals(0, streamWriter.appendCount);
        assertEquals(0, streamWriter.successfulRowCount);
        assertEquals(1, streamWriter.closeCount);
        assertEquals(2, sinkWriter.buffer.length());
    }

    private static BigQuerySinkBatchWriter createSinkWriter(BigQueryWriter streamWriter) {
        return new BigQuerySinkBatchWriter(
                ReadonlyConfig.fromMap(Collections.emptyMap()), streamWriter, null);
    }

    private static AppendRowsResponse errorResponse(Code code) {
        return AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(code.getNumber())
                                .setMessage(code.name())
                                .build())
                .build();
    }

    private static Throwable offsetAlreadyExists(long expectedOffset, long receivedOffset) {
        String message =
                String.format(
                        "The offset is within stream, expected offset %d, received %d",
                        expectedOffset, receivedOffset);
        StorageError storageError =
                StorageError.newBuilder()
                        .setCode(StorageError.StorageErrorCode.OFFSET_ALREADY_EXISTS)
                        .setEntity("projects/p/datasets/d/tables/t/streams/s")
                        .setErrorMessage(message)
                        .build();
        Status status =
                Status.newBuilder()
                        .setCode(Code.ALREADY_EXISTS.getNumber())
                        .setMessage(message)
                        .addDetails(Any.pack(storageError))
                        .build();
        Throwable error = Exceptions.toStorageException(status, null);
        assertTrue(error instanceof Exceptions.OffsetAlreadyExists);
        return new java.util.concurrent.ExecutionException(error);
    }

    private static class SequencedBigQueryWriter implements BigQueryWriter {
        private final java.util.Deque<ApiFuture<AppendRowsResponse>> results =
                new java.util.ArrayDeque<>();
        private final java.util.List<JSONArray> appended = new java.util.ArrayList<>();
        private int successfulRowCount;

        @SafeVarargs
        private SequencedBigQueryWriter(ApiFuture<AppendRowsResponse>... results) {
            this.results.addAll(java.util.Arrays.asList(results));
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(JSONArray jsonArr) {
            appended.add(jsonArr);
            return results.removeFirst();
        }

        @Override
        public void onAppendSuccess(int rowCount) {
            successfulRowCount += rowCount;
        }

        @Override
        public void close() {}

        @Override
        public String getStreamName() {
            return "test-stream";
        }
    }

    private static StatusCode statusCode(StatusCode.Code code) {
        return new StatusCode() {
            @Override
            public StatusCode.Code getCode() {
                return code;
            }

            @Override
            public Object getTransportCode() {
                return null;
            }
        };
    }

    private static class TestingBigQueryWriter implements BigQueryWriter {
        private final ApiFuture<AppendRowsResponse> result;
        private int appendCount;
        private int successfulRowCount;
        private int closeCount;

        private TestingBigQueryWriter(ApiFuture<AppendRowsResponse> result) {
            this.result = result;
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(JSONArray jsonArr) {
            appendCount++;
            return result;
        }

        @Override
        public void onAppendSuccess(int rowCount) {
            successfulRowCount += rowCount;
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public String getStreamName() {
            return "test-stream";
        }
    }
}
