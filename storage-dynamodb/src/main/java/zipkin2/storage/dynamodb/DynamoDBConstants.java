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
  static final String SEARCH_TABLE_BASE_NAME = "search";

  static final String FIELD_DELIMITER = "░";

  static final String WILDCARD_FOR_INVERTED_INDEX_LOOKUP = "__ANY__";

  static final String TTL_COLUMN = "ttl";

  static final class Spans {
    static final String TRACE_ID = "trace_id";
    static final String SPAN_TIMESTAMP_ID = "span_timestamp_id";
    static final String TRACE_ID_64 = "trace_id_64";
    static final String SPAN_ID = "span_id";
    static final String SPAN_NAME = "span_name";
    static final String SPAN_BLOB = "span_blob";
    static final String LOCAL_SERVICE_NAME = "local_service_name";
    static final String LOCAL_SERVICE_SPAN_NAME = "local_service_span_name";
    static final String REMOTE_SERVICE_NAME = "remote_service_name";
    static final String REMOTE_SERVICE_SPAN_NAME = "remote_service_span_name";
    static final String SPAN_TIMESTAMP = "span_timestamp";
    static final String SPAN_DURATION = "span_duration";
    static final String TAG_PREFIX = "tag.";
    static final String ANNOTATIONS = "annotations";
  }

  static final class Search {
    static final String KEY_INDEX = "key_index";
    static final String VALUE_INDEX = "value_index";

    static final String ENTITY_TYPE = "entity_type";
    static final String ENTITY_KEY_VALUE = "entity_key_value";

    static final String ENTITY_KEY = "entity_key";
    static final String ENTITY_VALUE = "entity_value";

    static final String SERVICE_SPAN_ENTITY_TYPE = "service-span";
    static final String AUTOCOMPLETE_TAG_ENTITY_TYPE = "autocomplete-tag";

    static final String UNKNOWN = "unknown";
  }
}
