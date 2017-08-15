package dev.yn.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Metadata
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.natpryce.konfig.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Encapsulates all cassandra connection logic.  Maintains a single session for use across the entire application.
 *
 * See datastax documentation:
 * http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra
 */
class CassandraConnector(val clusterBuilder: Cluster.Builder,
                         val CassandraConnectionRetryIntervalMillis: Long = 500,
                         val CassandraConnectionRetryIntervalMillisMax: Long = 30000) {
    private val hostNames: List<String> by lazy {
        clusterBuilder.contactPoints.map { it.hostName }
    }

    companion object {
        val singleton: CassandraConnector by lazy {
            ConfigLoader.loadRequiredProperty(CassandraConfig.cassandra.nodes)
                .let { clusterBuilder(it) }
                .let { CassandraConnector(it) }
        }

        fun clusterBuilder(nodeAddresses: List<String>): Cluster.Builder {
            val builder = Cluster.builder()
            nodeAddresses.forEach {
                builder.addContactPoint(it)
            }
            return builder
        }
    }

    private val Log: Logger = LoggerFactory.getLogger(this.javaClass)

    private var currentCluster = buildCluster()
    private var currentSession = buildSession()

    val metadata = initMetadata()

    fun session(): Session {
        synchronized(this) {
            if (currentSession.isClosed) {
                currentSession = buildSession()
            }
            return currentSession
        }
    }

    fun reset(): Session {
        synchronized(this) {
            close()
            currentSession = buildSession()
            return currentSession
        }
    }

    fun close() {
        synchronized(this) {
            currentSession.close()
            currentCluster.close()
        }
    }

    private fun buildSession(): Session {
        return catchNoHostAndRetry({
            cluster().connect()
        })
    }

    private fun cluster(): Cluster {
        synchronized(this) {
            if (currentCluster.isClosed) {
                currentCluster = buildCluster()
            }
            return currentCluster
        }
    }

    private fun buildCluster(): Cluster {
        return catchNoHostAndRetry({
            clusterBuilder.build()
        })
    }

    private fun initMetadata(): Metadata {
        val meta = cluster().getMetadata()
        Log.info("Connected to cluster: ${meta.getClusterName()}")
        meta.getAllHosts().forEach {
            Log.info("Datacenter: ${it.getDatacenter()}; Host: ${it.getAddress()}; Rack: ${it.getRack()}")
        }
        return meta
    }

    private fun <T> catchNoHostAndRetry(action: () -> T, interval: Long = CassandraConnectionRetryIntervalMillis): T {
        synchronized(this) {
            try {
                return action()
            } catch(noHost: NoHostAvailableException) {
                Log.error("could not connect to hosts: $hostNames, retrying in $interval")
                Thread.sleep(interval)
                return catchNoHostAndRetry(action, Math.min(interval * 2, CassandraConnectionRetryIntervalMillisMax))
            }
        }
    }
}