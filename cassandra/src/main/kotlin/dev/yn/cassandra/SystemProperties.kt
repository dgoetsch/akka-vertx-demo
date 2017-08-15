package dev.yn.cassandra

import com.natpryce.konfig.ConfigurationProperties
import com.natpryce.konfig.getValue
import com.natpryce.konfig.stringType

object SystemProperties {
    val profile by stringType
}