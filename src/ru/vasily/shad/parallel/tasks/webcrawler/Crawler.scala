package ru.vasily.shad.parallel.tasks.webcrawler

import java.net.URL
import collection.JavaConversions._
import collection.mutable.HashSet
import RichFile._
import java.io.File
import scala.actors.Actor._
import scala.actors.Futures._
import scala.actors.Future

object Crawler {

  case class WriteFile(path: File, content: String)

  case class Stop()

  val processedUrlsSet = HashSet[String]()
  val fileWritingActor = actor {
    loop {
      react {
        case WriteFile(path, content) => {
          path.text = content
        }
        case Stop => exit('stop)
      }
    }
  }

  case class FuturesTree(futureTrees: Iterable[Future[FuturesTree]]) {
    def waitForAll() {
      for (futureTree <- futureTrees) {
        futureTree().waitForAll()
      }
    }
  }

  def fetchUrlRecursively(url: URL, depth: Int, outputDir: File): Future[FuturesTree] = future {
    if (depth == 0 || checkIsUrlProcessedAndSetAsProcessed(url)) {
      FuturesTree(Nil)
    } else {
      fetchContent(url) match {
        case Some(Content(html, links)) => {
          println("content from %s = %s".format(url, html))
          writeUrlContentToDisc(url, html, outputDir)
          val futures = for (innerUrl <- links) yield fetchUrlRecursively(innerUrl, depth - 1, outputDir)
          FuturesTree(futures)
        }
        case None => FuturesTree(Nil)
      }
    }
  }

  def writeUrlContentToDisc(url: URL, html: String, outputDir: File) {
    val path: File = new File(outputDir, url.toString.replace("/", "#") + ".html")
    fileWritingActor ! WriteFile(path, html)
  }

  case class Content(html: String, links: Iterable[URL])

  def fetchContent(url: URL): Option[Content] = {
    val html = CrawlerUtils.getContent(url)
    if (html == null) {
      None
    } else {
      val links = CrawlerUtils.getLinks(url, html).toSeq
      Some(Content(html, links))
    }
  }

  def checkIsUrlProcessedAndSetAsProcessed(url: URL): Boolean = {
    Crawler synchronized {
      val isProcessed = processedUrlsSet.contains(url.toString)
      processedUrlsSet.add(url.toString);
      isProcessed
    }
  }

  def timeOfExecution(operation: => Unit): Long = {
    val startTime = System.currentTimeMillis()
    operation
    System.currentTimeMillis() - startTime
  }

  def main(args: Array[String]) {
    val time = timeOfExecution {
      val url: URL = new URL("http://board.rt.mipt.ru/")
      val depth = 2
      fetchUrlRecursively(url, depth, new File("output"))().waitForAll()
    }
    println(time)
    fileWritingActor ! Stop
  }
}
