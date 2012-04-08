package ru.vasily.shad.parallel.tasks.webcrawler

import scala.io._
import java.io.{File, PrintWriter, FileWriter}


class RichFile(file: File) {

  def text = Source.fromFile(file).mkString

  def text_=(s: String) {
    val out = new PrintWriter(new FileWriter(file))
    try {
      out.print(s)
    }
    finally {
      out.close
    }
  }
}

object RichFile {
  implicit def enrichFile(file: File) = new RichFile(file)
}
