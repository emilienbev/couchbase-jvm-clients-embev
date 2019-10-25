/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.scala.search.result

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.search.SearchChunkRow
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer}
import com.couchbase.client.scala.json.JsonObjectSafe
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** The results of an FTS query.
  *
  * @param errors          any execution error that happened.  Note that it is possible to have both rows and errors.
  * @param facets          any search facets returned
  * @param metaData            any additional information related to the FTS query
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class SearchResult(
    private[scala] val _rows: Seq[SearchRow],
    errors: Seq[RuntimeException],
    metaData: SearchMetaData
) {

  /** All returned rows.  All rows are buffered from the FTS service first.
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error
    */
  def rows: Seq[SearchRow] = _rows
}

/** The results of an FTS query, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the FTS service returns an error while returning the
  *                        rows, it will be raised on this Flux
  * @param meta            any additional information related to the FTS query
  */
case class ReactiveSearchResult(
    private[scala] val rows: SFlux[SearchChunkRow],
    meta: SMono[SearchMetaData]
) {

  /** Return all rows, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def rowsAs[T](implicit deserializer: JsonDeserializer[T]): SFlux[T] = {
    rows.map(
      row =>
        deserializer.deserialize(row.data()) match {
          case Success(v)   => v
          case Failure(err) => throw err
        }
    )
  }
}

/** Metrics of a given FTS request.
  *
  * @param took        how long a request took executing on the server side
  * @param totalRows   number of rows returned
  * @param maxScore    the largest score amongst the rows.
  */
case class SearchMetrics(took: Duration, totalRows: Long, maxScore: Double)

/** Represents the status of a FTS query.
  *
  * @param totalCount   the total number of FTS pindexes that were queried.
  * @param successCount the number of FTS pindexes queried that successfully answered.
  * @param errorCount   the number of FTS pindexes queried that gave an error. If &gt; 0,
  */
case class SearchStatus(totalCount: Long, successCount: Long, errorCount: Long) {

  /** If all FTS indexes answered successfully. */
  def isSuccess: Boolean = errorCount == 0
}

/** Additional information returned by the FTS service after any rows and errors.
  *
  * @param metrics         metrics related to the FTS request, if they are available
  * @param warnings        any warnings returned from the FTS service
  * @param status          the raw status string returned from the FTS service
  */
case class SearchMetaData(status: SearchStatus, metrics: SearchMetrics)

private[scala] object SearchStatus {
  def fromBytes(in: Array[Byte]): SearchStatus = {
    JsonObjectSafe.fromJson(new String(in, CharsetUtil.UTF_8)) match {
      case Success(jo) =>
        SearchStatus(
          jo.numLong("total").getOrElse(0),
          jo.numLong("successful").getOrElse(0),
          jo.numLong("failed").getOrElse(0)
        )

      case Failure(err) =>
        SearchStatus(0, 0, 0)
    }
  }
}
