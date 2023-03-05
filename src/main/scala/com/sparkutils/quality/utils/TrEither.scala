package com.sparkutils.quality.utils

// scala 3 unions can't come fast enough :)
sealed trait TrEither[+A, +B, +C] {
  def isA: Boolean
  def isB: Boolean
  def isC: Boolean

  def getA: A
  def getB: B
  def getC: C

  def fold[R](a: A => R, b: B => R, c: C => R): R
}

case class Tr1[A](a: A) extends TrEither[A, Nothing, Nothing]{
  val isA: Boolean = true
  val isB: Boolean = false
  val isC: Boolean = false

  val getA: A = a
  def getB: Nothing = ???
  def getC: Nothing = ???

  override def fold[R](a: A => R, b: Nothing => R, c: Nothing => R): R = a(getA)
}

case class Tr2[B](b: B) extends TrEither[Nothing, B, Nothing]{
  val isA: Boolean = false
  val isB: Boolean = true
  val isC: Boolean = false

  def getA: Nothing = ???
  val getB: B = b
  def getC: Nothing = ???

  override def fold[R](a: Nothing => R, b: B => R, c: Nothing => R): R = b(getB)
}

case class Tr3[C](c: C) extends TrEither[Nothing, Nothing, C]{
  val isA: Boolean = false
  val isB: Boolean = false
  val isC: Boolean = true

  def getA: Nothing = ???
  def getB: Nothing = ???
  val getC: C = c

  override def fold[R](a: Nothing => R, b: Nothing => R, c: C => R): R = c(getC)
}

