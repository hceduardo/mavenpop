package com.redhat.mavenpop

object TransactionFailureReason extends Enumeration {
  type TransactionFailureReason = Value
  val TIMEOUT, OTHER = Value
}
