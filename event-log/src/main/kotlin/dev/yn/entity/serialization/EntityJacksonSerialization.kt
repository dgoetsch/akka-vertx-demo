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

package dev.yn.entity.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import dev.yn.entity.domain.EntityError
import dev.yn.event.serialization.JacksonSerialization

open class EntityJacksonSerialization<T>(objectMapper: ObjectMapper, override val clazz: Class<T>): JacksonSerialization<EntityError, T>(objectMapper) {
    override val jsonError: (Throwable) -> EntityError = { EntityError.JsonError(it) }
}