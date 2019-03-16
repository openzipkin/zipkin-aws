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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;

import static zipkin2.storage.dynamodb.DynamoDBConstants.AUTOCOMPLETE_TAGS_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.TAG;
import static zipkin2.storage.dynamodb.DynamoDBConstants.AutocompleteTags.VALUE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.SPANS_TABLE_BASE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SERVICE;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.SPAN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.ServiceSpanNames.UNKNOWN;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.ANNOTATIONS;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.DURATION;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.LOCAL_SERVICE_NAME_SPAN_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME;
import static zipkin2.storage.dynamodb.DynamoDBConstants.Spans.REMOTE_SERVICE_NAME_SPAN_NAME;
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

  private final List<String> autocompleteKeys;
  private final AmazonDynamoDB dynamoDB;
  private final String spansTableName;
  private final String serviceSpanNamesTableName;
  private final String autocompleteTagsTableName;

  DynamoDBSpanConsumer(DynamoDBStorage.Builder builder) {
    this.autocompleteKeys = builder.autocompleteKeys;
    this.dynamoDB = builder.dynamoDB;

    this.spansTableName = builder.tablePrefix + SPANS_TABLE_BASE_NAME;
    this.serviceSpanNamesTableName = builder.tablePrefix + SERVICE_SPAN_NAMES_TABLE_BASE_NAME;
    this.autocompleteTagsTableName = builder.tablePrefix + AUTOCOMPLETE_TAGS_TABLE_BASE_NAME;
  }

  @Override public Call<Void> accept(List<Span> list) {
    List<WriteRequest> spanWriteRequests = new ArrayList<>(createWriteSpans(list));

    while (!spanWriteRequests.isEmpty()) {
      int maxIndex = Math.min(spanWriteRequests.size(), 25);
      BatchWriteItemRequest request = new BatchWriteItemRequest();
      request.addRequestItemsEntry(spansTableName, spanWriteRequests.subList(0, maxIndex));

      dynamoDB.batchWriteItem(request);
      spanWriteRequests.subList(0, maxIndex).clear();
    }

    List<UpdateItemRequest> names = createUpdateNames(list);
    for (UpdateItemRequest request : names) {
      dynamoDB.updateItem(request.withTableName(serviceSpanNamesTableName));
    }

    List<UpdateItemRequest> autocompleteTags = createAutocompleteTags(list);
    for (UpdateItemRequest request : autocompleteTags) {
      dynamoDB.updateItem(request.withTableName(autocompleteTagsTableName));
    }

    return Call.create(null);
  }

  private List<WriteRequest> createWriteSpans(List<Span> spans) {
    List<WriteRequest> result = new ArrayList<>(spans.size());
    for (Span span : spans) {
      PutRequest spanPut = new PutRequest();
      spanPut.addItemEntry(TRACE_ID, new AttributeValue(span.traceId()));
      spanPut.addItemEntry(TRACE_ID_64,
          new AttributeValue(span.traceId().substring(Math.max(0, span.traceId().length() - 16))));

      spanPut.addItemEntry(TIMESTAMP_SPAN_ID,
          new AttributeValue().withN(
              DynamoDBStorage.timestampId(span.timestampAsLong() / 1000, span.id())));

      spanPut.addItemEntry(SPAN_ID, new AttributeValue().withS(span.id()));

      if (span.name() != null && !span.name().isEmpty()) {
        spanPut.addItemEntry(SPAN_NAME, new AttributeValue().withS(span.name()));

        if (span.localServiceName() != null && !span.localServiceName().isEmpty()) {
          spanPut.addItemEntry(LOCAL_SERVICE_NAME_SPAN_NAME,
              new AttributeValue().withS(span.localServiceName() + "###" + span.name()));
        }

        if (span.remoteServiceName() != null && !span.remoteServiceName().isEmpty()) {
          spanPut.addItemEntry(REMOTE_SERVICE_NAME_SPAN_NAME,
              new AttributeValue().withS(span.remoteServiceName() + "###" + span.name()));
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

      spanPut.addItemEntry(TTL_COLUMN, ttlForSpan(span));

      result.add(new WriteRequest(spanPut));
    }
    return result;
  }

  private List<UpdateItemRequest> createUpdateNames(List<Span> spans) {
    PairWithTTL.PairWithTTLBuilder reusableBuilder = PairWithTTL.newBuilder(SERVICE, SPAN);
    List<PairWithTTL> pairs = new ArrayList<>();

    for (Span span : spans) {
      AttributeValue ttlForSpan = ttlForSpan(span);
      PairWithTTL.PairWithTTLBuilder localServiceBuilder = PairWithTTL.newBuilder(SERVICE, SPAN);

      if (span.localServiceName() != null && !span.localServiceName().isEmpty()) {
        pairs.add(reusableBuilder.build(span.localServiceName(), WILDCARD_FOR_INVERTED_INDEX_LOOKUP,
            ttlForSpan));

        localServiceBuilder.keyValue(span.localServiceName());
      } else {
        localServiceBuilder.keyValue(UNKNOWN);
      }

      if (span.name() != null && !span.name().isEmpty()) {
        localServiceBuilder.valueValue(span.name());
      } else {
        localServiceBuilder.valueValue(UNKNOWN);
      }

      if (span.remoteServiceName() != null && !span.remoteServiceName().isEmpty()) {
        pairs.add(
            reusableBuilder.build(span.remoteServiceName(), WILDCARD_FOR_INVERTED_INDEX_LOOKUP,
                ttlForSpan));
        pairs.add(reusableBuilder.build(span.remoteServiceName(), localServiceBuilder.valueValue,
            ttlForSpan));
      }

      localServiceBuilder.ttl(ttlForSpan);

      pairs.add(localServiceBuilder.build());
    }

    PairWithTTL.merge(pairs);

    return pairs.stream().map(PairWithTTL::asUpdateItemRequest).collect(Collectors.toList());
  }

  private List<UpdateItemRequest> createAutocompleteTags(List<Span> spans) {
    PairWithTTL.PairWithTTLBuilder builder = PairWithTTL.newBuilder(TAG, VALUE);
    List<PairWithTTL> pairs = new ArrayList<>();

    for (Span span : spans) {
      AttributeValue ttlForSpan = ttlForSpan(span);
      for (Map.Entry<String, String> tag : span.tags().entrySet()) {
        if (autocompleteKeys.contains(tag.getKey())) {
          pairs.add(builder.build(tag.getKey(), tag.getValue(), ttlForSpan));
          pairs.add(
              builder.build(tag.getKey(), WILDCARD_FOR_INVERTED_INDEX_LOOKUP, ttlForSpan));
        }
      }
    }
    PairWithTTL.merge(pairs);

    return pairs.stream().map(PairWithTTL::asUpdateItemRequest).collect(Collectors.toList());
  }

  private AttributeValue ttlForSpan(Span span) {
    return new AttributeValue().withN(String.valueOf(span.timestampAsLong()));
  }

  private static class PairWithTTL {
    private final String key_column;
    private final String key_value;
    private final String value_column;
    private final String value_value;
    private final AttributeValue ttl;

    private PairWithTTL(String key_column, String key_value, String value_column,
        String value_value, AttributeValue ttl) {
      this.key_column = key_column;
      this.key_value = key_value;
      this.value_column = value_column;
      this.value_value = value_value;
      this.ttl = ttl;
    }

    static PairWithTTLBuilder newBuilder(String key_column, String value_column) {
      return new PairWithTTLBuilder(key_column, value_column);
    }

    static void merge(List<PairWithTTL> input) {
      Map<String, Map<String, PairWithTTL>> found = new HashMap<>();
      List<PairWithTTL> toRemove = new ArrayList<>();
      for (PairWithTTL pair : input) {
        if (found.containsKey(pair.key_value)) {
          if (found.get(pair.key_value).containsKey(pair.value_value)) {
            PairWithTTL toMergeWith = found.get(pair.key_value).get(pair.value_value);
            if (Long.valueOf(pair.ttl.getN()) > Long.valueOf(toMergeWith.ttl.getN())) {
              toMergeWith.ttl.withN(pair.ttl.getN());
            }
            toRemove.add(pair);
          } else {
            found.get(pair.key_value).put(pair.value_value, pair);
          }
        } else {
          found.put(pair.key_value, new HashMap<>());
          found.get(pair.key_value).put(pair.value_value, pair);
        }
      }
      input.removeAll(toRemove);
    }

    private UpdateItemRequest asUpdateItemRequest() {
      return new UpdateItemRequest()
          .withKey(new HashMap<String, AttributeValue>() {{
            put(key_column, new AttributeValue().withS(key_value));
            put(value_column, new AttributeValue().withS(value_value));
          }})
          .addAttributeUpdatesEntry(TTL_COLUMN, new AttributeValueUpdate().withValue(ttl));
    }

    private static class PairWithTTLBuilder {
      private final String key_column;
      private final String value_column;
      private String keyValue;
      private String valueValue;
      private AttributeValue ttl;

      private PairWithTTLBuilder(String key_column, String value_column) {
        this.key_column = key_column;
        this.value_column = value_column;
      }

      PairWithTTLBuilder keyValue(String keyValue) {
        this.keyValue = keyValue;
        return this;
      }

      PairWithTTLBuilder valueValue(String valueValue) {
        this.valueValue = valueValue;
        return this;
      }

      PairWithTTLBuilder ttl(AttributeValue ttl) {
        this.ttl = ttl;
        return this;
      }

      PairWithTTL build(String key, String value, AttributeValue ttl) {
        return new PairWithTTL(key_column, key, value_column, value, ttl);
      }

      PairWithTTL build() {
        return new PairWithTTL(key_column, keyValue, value_column, valueValue, ttl);
      }
    }
  }
}
