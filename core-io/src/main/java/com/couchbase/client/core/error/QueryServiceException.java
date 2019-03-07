/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.error;

/**
 * There was a problem fulfilling the query request.
 *
 * Check <code>content()</code> for further details.
 *
 * @author Graham Pople
 * @since 2.0.0
 */
public class QueryServiceException extends CouchbaseException {
    private final byte[] content;

    public QueryServiceException(byte[] content) {
        super();
        this.content = content;
    }

    public QueryServiceException(String message, byte[] content) {
        super(message);
        this.content = content;
    }

    public QueryServiceException(String message, Throwable cause, byte[] content) {
        super(message, cause);
        this.content = content;
    }

    public QueryServiceException(Throwable cause, byte[] content) {
        super(cause);
        this.content = content;
    }

    public byte[] content() {
        return content;
    }
}
