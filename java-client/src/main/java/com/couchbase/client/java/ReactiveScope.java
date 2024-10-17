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

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreSearchRequest;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.error.context.ReducedSearchErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.util.ReactorOps;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.eventing.ReactiveScopeEventingFunctionManager;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.vector.VectorSearch;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_SEARCH_OPTIONS;

/**
 * The scope identifies a group of collections and allows high application
 * density as a result.
 *
 * <p>If no scope is explicitly provided, the default scope is used.</p>
 *
 * @since 3.0.0
 */
public class ReactiveScope {

  /**
   * The underlying async scope which actually performs the actions.
   */
  private final AsyncScope asyncScope;

  private final ReactorOps reactor;

  /**
   * Stores already opened collections for reuse.
   */
  private final Map<String, ReactiveCollection> collectionCache = new ConcurrentHashMap<>();

  /**
   * Creates a new {@link ReactiveScope}.
   *
   * @param asyncScope the underlying async scope.
   */
  ReactiveScope(final AsyncScope asyncScope) {
    this.reactor = asyncScope.environment();
    this.asyncScope = asyncScope;
  }

  /**
   * The name of the scope.
   *
   * @return the name of the scope.
   */
  public String name() {
    return asyncScope.name();
  }

  /**
   * The name of the bucket this scope is attached to.
   */
  public String bucketName() {
    return asyncScope.bucketName();
  }

  /**
   * Returns the underlying async scope.
   */
  public AsyncScope async() {
    return asyncScope;
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return asyncScope.core();
  }

  /**
   * Provides access to the configured {@link ClusterEnvironment} for this scope.
   */
  public ClusterEnvironment environment() {
    return asyncScope.environment();
  }

  /**
   * Opens the default collection for this scope.
   *
   * @return the default collection once opened.
   */
  ReactiveCollection defaultCollection() {
    return collectionCache.computeIfAbsent(
      CollectionIdentifier.DEFAULT_COLLECTION,
      n -> new ReactiveCollection(asyncScope.defaultCollection())
    );
  }

  /**
   * Opens a collection for this scope with an explicit name.
   *
   * @param collectionName the collection name.
   * @return the requested collection if successful.
   */
  public ReactiveCollection collection(final String collectionName) {
    return collectionCache.computeIfAbsent(collectionName, n -> new ReactiveCollection(asyncScope.collection(n)));
  }

  /**
   * Performs a N1QL query with default {@link QueryOptions} in a Scope
   *
   * @param statement the N1QL query statement as a raw string.
   * @return the {@link ReactiveQueryResult} once the response arrives successfully.
   */
  public Mono<ReactiveQueryResult> query(final String statement) {
    return this.query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions} in a Scope
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link ReactiveQueryResult} once the response arrives successfully.
   */
  public Mono<ReactiveQueryResult> query(final String statement, final QueryOptions options) {
    notNull(options, "QueryOptions", () -> new ReducedQueryErrorContext(statement));
    final QueryOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return reactor.publishOnUserScheduler(async().queryOps.queryReactive(statement, opts, asyncScope.queryContext, null, QueryAccessor::convertCoreQueryError))
      .map(result -> new ReactiveQueryResult(result, serializer));
  }

  /**
   * Performs an Analytics query with default {@link AnalyticsOptions} on a scope
   *
   * @param statement the Analytics query statement as a raw string.
   * @return the {@link ReactiveAnalyticsResult} once the response arrives successfully.
   */
  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement) {
    return analyticsQuery(statement, DEFAULT_ANALYTICS_OPTIONS);
  }


  /**
   * Performs an Analytics query with custom {@link AnalyticsOptions} on a scope
   *
   * @param statement the Analytics query statement as a raw string.
   * @param options the custom options for this analytics query.
   * @return the {@link ReactiveAnalyticsResult} once the response arrives successfully.
   */
  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement, final AnalyticsOptions options) {
    notNull(options, "AnalyticsOptions", () -> new ReducedAnalyticsErrorContext(statement));
    AnalyticsOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return reactor.publishOnUserScheduler(Mono.defer(() -> {
      return AnalyticsAccessor.analyticsQueryReactive(
          asyncScope.core(),
          asyncScope.analyticsRequest(statement, opts),
          serializer
      );
    }));
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with default {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for scoped FTS indexes.  To work with global indexes, use {@link ReactiveCluster} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link Mono}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @SinceCouchbase("7.6")
  public Mono<ReactiveSearchResult> search(final String indexName, final SearchRequest searchRequest) {
    return search(indexName, searchRequest, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with custom {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for scoped FTS indexes.  To work with global indexes, use {@link ReactiveCluster} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link Mono}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @SinceCouchbase("7.6")
  public Mono<ReactiveSearchResult> search(final String indexName, final SearchRequest searchRequest, final SearchOptions options) {
    notNull(searchRequest, "SearchRequest", () -> new ReducedSearchErrorContext(indexName, null));
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, null));
    CoreSearchRequest coreRequest = searchRequest.toCore();
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return reactor.publishOnUserScheduler(asyncScope.searchOps.searchReactive(indexName, coreRequest, opts))
      .map(r -> new ReactiveSearchResult(r, serializer));
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   * <p>
   * This method is for scoped FTS indexes.  To work with global indexes, use {@link ReactiveCluster} instead.
   * <p>
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link ReactiveSearchResult} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(final String indexName, SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   * <p>
   * This method is for scoped FTS indexes.  To work with global indexes, use {@link ReactiveCluster} instead.
   * <p>
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link ReactiveSearchResult} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery", () -> new ReducedSearchErrorContext(indexName, null));
    CoreSearchQuery coreQuery = query.toCore();
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, coreQuery));
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return reactor.publishOnUserScheduler(asyncScope.searchOps.searchQueryReactive(indexName, coreQuery, opts))
            .map(result -> new ReactiveSearchResult(result, serializer));
  }

  /**
   * Provides access to the eventing function management services for functions in this scope.
   */
  @Stability.Volatile
  @SinceCouchbase("7.1")
  public ReactiveScopeEventingFunctionManager eventingFunctions() {
    return new ReactiveScopeEventingFunctionManager(environment(), asyncScope.eventingFunctions());
  }
}
