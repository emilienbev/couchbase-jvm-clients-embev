/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.manager.eventing;

/**
 * Defines the function language compatibility level.
 */
public enum EventingFunctionLanguageCompatibility {
  /**
   * Uses Server 6.0.0 language compat level.
   */
  VERSION_6_0_0 {
    @Override
    public String toString() {
      return "6.0.0";
    }
  },
  /**
   * Uses Server 6.5.0 language compat level.
   */
  VERSION_6_5_0 {
    @Override
    public String toString() {
      return "6.5.0";
    }
  },
  /**
   * Uses Server 6.6.2 language compat level.
   */
  VERSION_6_6_2 {
    @Override
    public String toString() {
      return "6.6.2";
    }
  }
}
