package com.redhat.mavenpop

object ExperimentProfile {

  def compute(): Int = {
    return 1
  }
  def main(args: Array[String]): Unit = {

    //get result only
    val result: Int = compute()

    //get result with instrumentation

    //withInstrumentaiton()
  }

}
