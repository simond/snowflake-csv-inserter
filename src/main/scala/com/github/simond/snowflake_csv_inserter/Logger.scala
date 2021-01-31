package com.github.simond.snowflake_csv_inserter

import java.io.{PrintWriter, StringWriter}

object Logger {
  def logStackTraceAndExit(exception: Throwable, mode: String => Unit): Unit = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))
    mode(sw.toString)
    System.exit(1)
  }
}
