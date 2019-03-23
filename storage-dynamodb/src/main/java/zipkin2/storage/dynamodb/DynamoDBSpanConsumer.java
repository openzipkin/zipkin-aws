/*
 * Copyright 2016-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.util.StringUtils;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;

import static zipkin2.storage.dynamodb.DynamoDBConstants.FIELD_DELIMITER;
import static zipkin2.storage.dynamodb.DynamoDBConstants.SEARCH_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.SPANS_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.AUTOCOMPLETE_TAG_ENTITY_TYPE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_KEY;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_KEY_VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.SERVICE_SPAN_ENTITY_TYPE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_TYPE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.UNKNOWN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Search.ENTITY_VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.ANNOTATIONS;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.DURATION;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_BLOB;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TAG_PREFIX;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TIMESTAMP;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TIMESTAMP_SPAN_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.TRACE_ID_64;
import static zipkin2.storage.dynamodb.DynamoDBConstants.TTL_COLUMN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.WILDCARD_FOR_INVERTED_INDEX_LOOKUP;

final class DynamoDBSpanConsumer implements SpanConsumer {
  private static Random random = new Random();

  private final List<String> autocompleteKeys;
  private final AmazonDynamoDB dynamoDB;
  private final long dataTtlSeconds;
  private final String spansTableName;
  private final String searchTableName;

  DynamoDBSpanConsumer(DynamoDBStorage.Builder builder) {
    this.autocompleteKeys = builder.autocompleteKeys;
    this.dynamoDB = builder.dynamoDB;
    this.dataTtlSeconds = builder.dataTtl.toMillis() / 1000;

    this.spansTableName = builder.tablePrefix + SPANS_TABLE_BASE_NAME;
    this.searchTableName = builder.tablePrefix + SEARCH_TABLE_BASE_NAME;
  }

  @Override public Call<Void> accept(List<Span> list) {
    AttributeValue ttlForBatch = new AttributeValue().withN(
        String.valueOf(Instant.now().getEpochSecond() + dataTtlSeconds));

    List<WriteRequest> spanWriteRequests = createWriteSpans(list, ttlForBatch);
    while (!spanWriteRequests.isEmpty()) {
      int maxIndex = Math.min(spanWriteRequests.size(), 25);
      BatchWriteItemRequest request = new BatchWriteItemRequest();
      request.addRequestItemsEntry(spansTableName, spanWriteRequests.subList(0, maxIndex));

      dynamoDB.batchWriteItem(request);
      spanWriteRequests.subList(0, maxIndex).clear();
    }

    // We use update requests for the next two entities because we want to update the TTL if they
    // exist already. Updates in DynamoDB are treated as upserts
    List<UpdateItemRequest> upserts = createUpsertsForServiceSpanNames(list, ttlForBatch);
    upserts.addAll(createUpsertsForAutocompleteTags(list, ttlForBatch));

    for (UpdateItemRequest request : upserts) {
      dynamoDB.updateItem(request.withTableName(searchTableName));
    }

    return Call.create(null);
  }

  private List<WriteRequest> createWriteSpans(List<Span> spans, AttributeValue ttl) {
    List<WriteRequest> result = new ArrayList<>(spans.size());
    for (Span span : spans) {
      PutRequest spanPut = new PutRequest();
      spanPut.addItemEntry(TRACE_ID, new AttributeValue(span.traceId()));
      spanPut.addItemEntry(TRACE_ID_64,
          new AttributeValue(span.traceId().substring(Math.max(0, span.traceId().length() - 16))));

      spanPut.addItemEntry(TIMESTAMP_SPAN_ID,
          new AttributeValue().withN(timestampId(span.timestampAsLong() / 1000)));

      spanPut.addItemEntry(SPAN_ID, new AttributeValue().withS(span.id()));

      if (span.name() != null && !span.name().isEmpty()) {
        spanPut.addItemEntry(SPAN_NAME, new AttributeValue().withS(span.name()));

        if (span.localServiceName() != null && !span.localServiceName().isEmpty()) {
          spanPut.addItemEntry(LOCAL_SERVICE_SPAN_NAME,
              new AttributeValue().withS(span.localServiceName() + FIELD_DELIMITER + span.name()));
        }

        if (span.remoteServiceName() != null && !span.remoteServiceName().isEmpty()) {
          spanPut.addItemEntry(REMOTE_SERVICE_SPAN_NAME,
              new AttributeValue().withS(span.remoteServiceName() + FIELD_DELIMITER + span.name()));
        }
      }

      if (span.localServiceName() != null && !span.localServiceName().isEmpty()) {
        spanPut.addItemEntry(LOCAL_SERVICE_NAME,
            new AttributeValue().withS(span.localServiceName()));
      }

      if (span.remoteServiceName() != null && !span.remoteServiceName().isEmpty()) {
        spanPut.addItemEntry(REMOTE_SERVICE_NAME,
            new AttributeValue().withS(span.remoteServiceName()));
      }

      spanPut.addItemEntry(SPAN_BLOB,
          new AttributeValue().withB(ByteBuffer.wrap(SpanBytesEncoder.PROTO3.encode(span))));

      span.tags().forEach((key, value) -> {
        if (value.isEmpty()) {
          spanPut.addItemEntry(TAG_PREFIX + key, new AttributeValue().withNULL(true));
        } else {
          spanPut.addItemEntry(TAG_PREFIX + key, new AttributeValue().withS(value));
        }
      });

      if (span.annotations() != null && !span.annotations().isEmpty()) {
        spanPut.addItemEntry(ANNOTATIONS, new AttributeValue().withSS(
            span.annotations().stream().map(Annotation::value).collect(Collectors.toList())
        ));
      }

      if (span.timestamp() != null) {
        spanPut.addItemEntry(TIMESTAMP,
            new AttributeValue().withN(String.valueOf(span.timestampAsLong())));
      }

      if (span.duration() != null) {
        spanPut.addItemEntry(DURATION,
            new AttributeValue().withN(String.valueOf(span.durationAsLong())));
      }

      spanPut.addItemEntry(TTL_COLUMN, ttl);

      result.add(new WriteRequest(spanPut));
    }
    return result;
  }

  private String timestampId(long timestamp) {
    return BigInteger.valueOf(timestamp).shiftLeft(Long.SIZE).add(new BigInteger(Long.SIZE, random))
        .toString();
  }

  private List<UpdateItemRequest> createUpsertsForServiceSpanNames(List<Span> spans, AttributeValue ttl) {
    Set<Pair> entries = new HashSet<>();

    for (Span span : spans) {
      String localServiceName = StringUtils.isNullOrEmpty(span.localServiceName()) ? UNKNOWN : span.localServiceName();
      String spanName = StringUtils.isNullOrEmpty(span.name()) ? UNKNOWN : span.name();
      entries.add(new Pair(localServiceName, spanName));

      if (!StringUtils.isNullOrEmpty(span.localServiceName())) {
        entries.add(new Pair(span.localServiceName(), WILDCARD_FOR_INVERTED_INDEX_LOOKUP));
      }

      if (!StringUtils.isNullOrEmpty(span.remoteServiceName())) {
        entries.add(new Pair(span.remoteServiceName(), spanName));
        entries.add(new Pair(span.remoteServiceName(), WILDCARD_FOR_INVERTED_INDEX_LOOKUP));
      }
    }

    return entries.stream()
        .map(p -> p.asUpdateItemRequest(SERVICE_SPAN_ENTITY_TYPE, ttl))
        .collect(Collectors.toList());
  }

  private List<UpdateItemRequest> createUpsertsForAutocompleteTags(List<Span> spans, AttributeValue ttl) {
    Set<Pair> entries = new HashSet<>();

    for (Span span : spans) {
      for (Map.Entry<String, String> tag : span.tags().entrySet()) {
        if (autocompleteKeys.contains(tag.getKey())) {
          entries.add(new Pair(tag.getKey(), tag.getValue()));
          entries.add(new Pair(tag.getKey(), WILDCARD_FOR_INVERTED_INDEX_LOOKUP));
        }
      }
    }

    return entries.stream()
        .map(p -> p.asUpdateItemRequest(AUTOCOMPLETE_TAG_ENTITY_TYPE, ttl))
        .collect(Collectors.toList());
  }

  private static class Pair implements Map.Entry<String, String> {
    private String key;
    private String value;

    Pair(String key, String value) {
      if (key == null) throw new IllegalArgumentException("Key cannot be null");
      if (value == null) throw new IllegalArgumentException("Value cannot be null");
      this.key = key;
      this.value = value;
    }

    UpdateItemRequest asUpdateItemRequest(String type, AttributeValue ttl) {
      return new UpdateItemRequest()
          .withKey(new HashMap<String, AttributeValue>() {{
            put(ENTITY_TYPE, new AttributeValue().withS(type));
            put(ENTITY_KEY_VALUE, new AttributeValue().withS(String.join(FIELD_DELIMITER, key, value)));
          }})
          .addAttributeUpdatesEntry(ENTITY_KEY, new AttributeValueUpdate().withValue(new AttributeValue().withS(key)))
          .addAttributeUpdatesEntry(ENTITY_VALUE, new AttributeValueUpdate().withValue(new AttributeValue().withS(value)))
          .addAttributeUpdatesEntry(TTL_COLUMN, new AttributeValueUpdate().withValue(ttl));
    }

    @Override public String getKey() {
      return key;
    }

    @Override public String getValue() {
      return value;
    }

    @Override public String setValue(String value) {
      this.value = value;
      return this.value;
    }

    @Override public int hashCode() {
      return key.hashCode() + value.hashCode();
    }

    @Override public boolean equals(Object other) {
      if (other instanceof Pair) {
        Pair otherPair = (Pair) other;
        return this.key.equals(otherPair.key) && this.value.equals(otherPair.value);
      } else {
        return false;
      }
    }
  }
}
