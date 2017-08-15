package dev.yn.cassandra

import com.natpryce.konfig.*
import org.funktionale.tries.Try


object ConfigLoader {
    val config: ConfigurationProperties by lazy {
        ConfigurationProperties.fromResource("application.${ConfigurationProperties.systemProperties()[SystemProperties.profile]}.conf")
    }

    val system: ConfigurationProperties by lazy {
        ConfigurationProperties.systemProperties()
    }

    fun <T> loadOptionalProperty(key: Key<T>): T? {
        return config.getOrNull(key) ?: system.getOrNull(key)
    }

    fun <T> loadRequiredProperty(key: Key<T>): T {
        return config.getOrNull(key) ?: system.get(key)
    }
}