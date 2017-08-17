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

package dev.yn.event.repository

import dev.yn.event.mother.aggregate.*
import io.kotlintest.Spec
import io.kotlintest.specs.StringSpec
import dev.yn.cassandra.CassandraConnector
import dev.yn.util.date.*
import io.kotlintest.matchers.shouldBe

class AggregateEventRepositorySpec: StringSpec() {
    override protected fun interceptSpec(context: Spec, spec: () -> Unit) {
        val connector = CassandraConnector.singleton
        val aggregateRepo = AggregateEventRepository(connector)
        aggregateRepo.initialize()

        aggregateEvents1.forEach { aggregateRepo.createIfNotExists(it) shouldBe true }
        aggregateEvents2.forEach { aggregateRepo.createIfNotExists(it) shouldBe true }

        spec()

        CassandraConnector.singleton.session().execute(AggregateEventCQL.dropTableStatement)
    }

    init {
        val aggregateRepo = AggregateEventRepository(CassandraConnector.singleton)


        "should fetch the most recent incorporate" {
            aggregateRepo.getLatest(resourceId1) shouldBe aggregateEvents1.last()
            aggregateRepo.getLatest(resourceId2) shouldBe aggregateEvents2.last()
        }

        "should get the events since a certain date" {
            aggregateRepo.getSince(resourceId1, dayFormat.parse("2017-05-03")) shouldBe aggregateEvents1.slice(2..7)
            aggregateRepo.getSince(resourceId2, dayFormat.parse("2017-05-02")) shouldBe aggregateEvents2.slice(1..7)
        }

        "should get the events in a date range" {
            aggregateRepo.get(resourceId1, dayFormat.parse("2017-05-03"), dayFormat.parse("2017-05-05")) shouldBe aggregateEvents1.slice(2..4)
            aggregateRepo.get(resourceId2, dayFormat.parse("2017-05-03"), dayFormat.parse("2017-05-07")) shouldBe aggregateEvents2.slice(1..5)
        }

        "should get the events for a user in a date range" {
            aggregateRepo.getForUser(resourceId1, user1, dayFormat.parse("2017-05-03"), dayFormat.parse("2017-05-05")) shouldBe
                    aggregateEvents1.slice(2..4)
                            .filter { it.userIds.contains(user1)}
            aggregateRepo.getForUser(resourceId2, user3, dayFormat.parse("2017-05-03"), dayFormat.parse("2017-05-07")) shouldBe
                    aggregateEvents2.slice(1..5)
                            .filter { it.userIds.contains(user3)}
        }


    }
}