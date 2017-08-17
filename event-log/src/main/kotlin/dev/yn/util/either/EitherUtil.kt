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

package dev.yn.util.either

import dev.yn.entity.domain.EntityError
import org.funktionale.either.Either
import org.funktionale.either.flatMap

data class AggregateResult<E, T>(val results: List<T>, val errors: List<E>)

fun <E, T> List<Either<E, T>>.aggregateResult(): AggregateResult<E, T> {
    return this.fold(AggregateResult<E, T>(emptyList(), emptyList())) { aggregate, either ->
        either.fold({ error -> aggregate.copy(errors = aggregate.errors + error) }, { result -> aggregate.copy(results = aggregate.results + result) })
    }
}

fun <T> Either<EntityError, T?>.nonNull(): Either<EntityError, T> {
    return this.right().flatMap {
        it?.let { Either.Right<EntityError, T>(it) } ?:
                Either.Left<EntityError, T>(EntityError.MissingField(""))
    }
}