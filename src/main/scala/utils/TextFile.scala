package utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object TextFile {
  def getTextFile: String = {
    try {
      val path = Paths.get(".\\text.txt")
      new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    } catch {
      case e: Exception =>
        println(e)
        println(s"Error reading file: ${e.getMessage}")
        ""
    }
  }
}
