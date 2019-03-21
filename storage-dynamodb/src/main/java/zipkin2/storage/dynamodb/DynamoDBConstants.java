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

final class DynamoDBConstants {
  static final String SPANS_TABLE_BASE_NAME = "spans";
  static final String SERVICE_SPAN_NAMES_TABLE_BASE_NAME = "service-span-names";
  static final String AUTOCOMPLETE_TAGS_TABLE_BASE_NAME = "autocomplete-tags";

  static final String WILDCARD_FOR_INVERTED_INDEX_LOOKUP = "__ANY__";

  static final String TTL_COLUMN = "ttl";

  final class Spans {
    static final String TRACE_ID = "trace_id";
    static final String TRACE_ID_64 = "trace_id_64";
    static final String TIMESTAMP_SPAN_ID = "timestamp_span_id";
    static final String SPAN_ID = "span_id";
    static final String SPAN_NAME = "span_name";
    static final String SPAN_BLOB = "span_blob";
    static final String LOCAL_SERVICE_NAME = "local_service_name";
    static final String LOCAL_SERVICE_NAME_SPAN_NAME = "local_service_name_span_name";
    static final String REMOTE_SERVICE_NAME = "remote_service_name";
    static final String REMOTE_SERVICE_NAME_SPAN_NAME = "remote_service_name_span_name";
    static final String TIMESTAMP = "timestamp";
    static final String DURATION = "duration";
    static final String TAG_PREFIX = "tag.";
    static final String ANNOTATIONS = "annotations";
  }

  final class ServiceSpanNames {
    static final String INDEX_INVERTED = "inverted";

    static final String SERVICE = "service_name";
    static final String SPAN = "span_name";

    static final String UNKNOWN = "unknown";
  }

  final class AutocompleteTags {
    static final String INDEX_INVERTED = "inverted";

    static final String TAG = "tag_key";
    static final String VALUE = "tag_value";
  }
}
