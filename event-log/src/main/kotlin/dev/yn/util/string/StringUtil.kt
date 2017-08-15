package dev.yn.util.string
import java.nio.ByteBuffer

fun String.toByteBuffer() = ByteBuffer.wrap(this.toByteArray())
