package com.redhat.mavenpop.DependencyParser

import scala.collection.mutable

class DependencyMap() {
  private val map = mutable.Map[String, scala.collection.mutable.Set[String]]()
  private val excludedDeps = Set[String]("UNKNOWN_DEPS", "NO_DEPS")

  def isEmpty: Boolean = map.isEmpty

  def iterator: Iterator[(String, mutable.Set[String])] = { map.iterator }

  def apply(gav: String) = map(gav)

  def contains(gav: String): Boolean = map.contains(gav)
  
  def add(gav: String, dependencies: Set[String]): Unit = {

    val _deps = dependencies -- excludedDeps

    _deps.foreach(dep => {
      if (!map.contains(dep)) {
        map(dep) = mutable.Set.empty[String]
      }
    })

    map.contains(gav) match {
      case false => map(gav) = mutable.Set(_deps.toSeq: _*)
      case true => map(gav) ++= _deps
    }
  }
}
