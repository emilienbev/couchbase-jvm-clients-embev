/*
 * Copyright (c) 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.metrics.micrometer;

import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.ValueRecorder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Implements the meter interface on top of the Micrometer {@link MeterRegistry}.
 */
public class MicrometerMeter implements Meter {

  private final MeterRegistry meterRegistry;

  private final Map<String, MicrometerCounter> counters = new ConcurrentHashMap<>();
  private final Map<String, MicrometerValueRecorder> valueRecorders = new ConcurrentHashMap<>();

  public static MicrometerMeter wrap(final MeterRegistry meterRegistry) {
    return new MicrometerMeter(meterRegistry);
  }

  private MicrometerMeter(final MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public Counter counter(final String name, final Map<String, String> tags) {
    return counters.computeIfAbsent(
      name,
      key -> new MicrometerCounter(meterRegistry.counter(key, convertTags(tags)))
    );
  }

  @Override
  public ValueRecorder valueRecorder(final String name, final Map<String, String> tags) {
    return valueRecorders.computeIfAbsent(
      name,
      key -> new MicrometerValueRecorder(meterRegistry.summary(key, convertTags(tags)))
    );
  }

  /**
   * Converts the generic tag map structure into the Micrometer {@link Tag}.
   *
   * @param tags the generic tags.
   * @return an iterable of micrometer tags.
   */
  private static Iterable<Tag> convertTags(final Map<String, String> tags) {
    return tags
      .entrySet()
      .stream()
      .map(e -> Tag.of(e.getKey(), e.getValue()))
      .collect(Collectors.toList());
  }

}
