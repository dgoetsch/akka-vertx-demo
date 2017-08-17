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

import com.datastax.driver.core.Cluster
import com.natpryce.konfig.*
import io.kotlintest.TestCaseContext
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.StringSpec
import org.mockito.Mockito.*

class CassandraConnectorSpec: StringSpec() {


    val builderMock = mock(Cluster.Builder::class.java)
    val clusterMock = mock(Cluster::class.java)
    val sessionMock = mock(Cluster::class.java)

    override protected fun interceptTestCase(context: TestCaseContext, test: () -> Unit) {
        //before
        reset(builderMock, clusterMock, sessionMock)

        //execute test
        test()

        //after

    }

    init {
        "cassandra connector should be configured" {
            ConfigLoader.loadRequiredProperty(SystemProperties.profile) shouldBe("test")
            ConfigLoader.loadRequiredProperty(CassandraConfig.cassandra.nodes) shouldBe(listOf("health", "safety"))
        }
    }




}
