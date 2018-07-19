package com.redhat.mavenpop.test

import java.net.ServerSocket

object TestHelpers {

  def generateMismatchMessage[T](actual: Array[T], expected: Array[T]): String = {

    val stringBuilder: StringBuilder = StringBuilder.newBuilder

    if (actual.size != expected.size) {

      actual.toSet -- expected.toSet

      stringBuilder ++= s"different sizes: actual size = ${actual.size} , expected size = ${expected.size} \n"
      stringBuilder ++= "actual contents: \n"
      stringBuilder ++= actual.mkString(",") + "\n"
      stringBuilder ++= "expected contents: \n"
      stringBuilder ++= expected.mkString(",") + "\n"
    } else {
      stringBuilder ++= "different elements: \n" + actual.zip(expected).flatMap {
        case (r1, r2) =>
          if (!r1.equals(r2)) {
            Some(s"$r1 | $r2")
          } else {
            None
          }
      }.mkString("\n")
    }
    stringBuilder.toString()
  }

  def getFreePort: Int = {
    var socket: ServerSocket = null
    var _port: Int = -1
    try {
      socket = new ServerSocket(0)
      socket.setReuseAddress(true)
      _port = socket.getLocalPort
    } finally {
      socket.close()
    }

    _port
  }
}
