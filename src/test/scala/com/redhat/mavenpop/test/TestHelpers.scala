package com.redhat.mavenpop.test

import java.net.ServerSocket

object TestHelpers {

  def generateMismatchMessage[T](a1: Array[T], a2: Array[T]): String = {
    if (a1.size != a2.size) {
      s"different sizes: ${a1.size} != ${a2.size}"
    } else {
      "different elements: \n" + a1.zip(a2).flatMap {
        case (r1, r2) =>
          if (!r1.equals(r2)) {
            Some(s"$r1 | $r2")
          } else {
            None
          }
      }.mkString("\n")
    }
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
