/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.columnar.client.java.internal;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.json.JsonMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Internal
public class JacksonTransformers {

  public static final ObjectMapper MAPPER = JsonMapper.builder()
    .addModule(new RepackagedJsonValueModule())
    .addModule(new AfterburnerModule())
    .build();

  private JacksonTransformers() {
    throw new AssertionError("not instantiable");
  }

}
