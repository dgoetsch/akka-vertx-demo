package dev.yn.fusion


object KotlinAdapters {
  implicit class FunktionaleEitherAdapter[L, R](val either: org.funktionale.either.Either[L, R]) {
    def toScala(): Either[L, R] = {
      if(either.isRight) {
        Right(either.right().get())
      } else {
        Left(either.left().get())
      }
    }

  }
}
