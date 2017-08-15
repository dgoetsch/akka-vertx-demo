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
