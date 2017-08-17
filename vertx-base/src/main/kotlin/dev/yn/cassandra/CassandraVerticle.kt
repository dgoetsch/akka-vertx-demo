/*
 *    Copyright 2017 Devyn Goetsch
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package dev.yn.cassandra

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import io.vertx.core.Future
import io.vertx.core.Vertx

class CassandraVerticle(val vertx: Vertx) {
    private val cassandraConnector by lazy {
        CassandraConnector.singleton
    }

    fun executeAsync(statement: Statement, callback: FutureCallback<ResultSet>): Unit {
        addCallback(cassandraConnector.session().executeAsync(statement), callback)
    }

    fun executeAsync(statement: Statement): Future<ResultSet> {
        val future = Future.future<ResultSet>()
        addCallback(cassandraConnector.session().executeAsync(statement), VertxFutureCallBack(future))
        return future;
    }

    private class VertxFutureCallBack(val future: Future<ResultSet>): FutureCallback<ResultSet> {
        override fun onFailure(t: Throwable?) {
            future.fail(t)
        }

        override fun onSuccess(result: ResultSet?) {
            future.complete(result)
        }
    }

    private fun <V> addCallback(future: ListenableFuture<V>, callback: FutureCallback<in V>): Unit {
        val context = vertx.getOrCreateContext()
        Futures.addCallback(future, callback, { command -> context.runOnContext({ aVoid -> command.run() }) })
    }
}
