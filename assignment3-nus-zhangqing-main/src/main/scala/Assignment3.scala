import scala.actors.A

/**
 * The Monad code and the Monad instances for Option and List are the same as in the lecture.
 */
trait Monad[F[_]]:
  def pure[A](a: A): F[A]
  def flatMap[A, B](ma: F[A])(f: A => F[B]): F[B]
  def map[A, B](ma: F[A])(f: A => B): F[B] = flatMap(ma)(a => pure(f(a)))
  def withFilter[A](ma: F[A])(f: A => Boolean): F[A]


extension [F[_], A](ma: F[A])(using m: Monad[F]) {
  def map[B](f: A => B): F[B] = m.map(ma)(f)
  def flatMap[B](f: A => F[B]): F[B] = m.flatMap(ma)(f)
  def withFilter(f: A => Boolean): F[A] = m.withFilter(ma)(f)
}

given listIsAMonad: Monad[List] = new Monad[List]:
  def pure[A](a: A): List[A] = List(a)
  def flatMap[A, B](ma: List[A])(f: A => List[B]): List[B] = ma.flatMap(f)
  def withFilter[A](ma: List[A])(f: A => Boolean): List[A] = ma.filter(f)

given optionIsAMonad: Monad[Option] = new Monad[Option]:
  def pure[A](a: A): Option[A] = Option(a)
  def flatMap[A, B](ma: Option[A])(f: A => Option[B]): Option[B] = ma.flatMap(f)
  def withFilter[A](ma: Option[A])(f: A => Boolean): Option[A] = ma.filter(f)

/**
 * The first monadic computation example. In every `for` comprehension, there
 * is an implicit `flatMap` call between every 2 lines of code. By injecting
 * a custom Monad (the one you are supposed to build), we can find out how
 * many times flatMap is invoked during the computation.
 */
def greatestCommonDivisor[F[_]: Monad](numbers: F[(Int, Int)]): F[Int] =
  val monad = summon[Monad[F]]
  import monad.pure
  Iterator.iterate(numbers) {
    state =>
      for
        (a, b) <- state
        a <- pure(if b != 0 then a % b else a)
        b <- pure(if a != 0 then b % a else b)
        flatMapCount += 1
      yield (a, b)
  }.dropWhile {
    state => state.map{(a,b) => a != 0 && b != 0} == pure(true)
  }.next.map{(a,b) => if a == 0 then b else a}


/**
 * This is the second monadic computation example. Same use as for the first one.
 */
def leastCommonMultiple[F[_] : Monad](numbers: F[(Int, Int)]): F[Int] =
  val monad = summon[Monad[F]]
  import monad.pure
  val input = numbers.map{(a, b) => (Math.max(a, b), Math.min(a, b))}
  Iterator.iterate(pure(1)) { count =>
    for k <- count yield k + 1
  }.dropWhile { count =>
    val notTerminal =
      for k <- count
        (a, b) <- input
        flatMapCount += 1
      yield a * k % b != 0
    notTerminal == pure(true)
  }.next.flatMap(k => input.map{(a, _) => a * k})

/**
 * You need to implement this Monad. It is similar to Option for the most part.
 * However, it's flatMap method has an ability to increment a counter so that
 * we can find out, for a given monadic computation, how many times flatMap was
 * invoked, and therefore have a measure of how long the computation took to
 * execute.
 */
/*case class TraceM[A](value: A):
  def map[B](f: A => B): TraceM[B] = a.map(ma)(f)

  def flatMap[B](f: A => TraceM[B]): TraceM[B] =

  def withFilter(f: A => Boolean): Unit =*/

/**
 * This is the companion object for your Monad, where the Monad's state can be
 * placed. You will need setters and getters for that state.
 */
object TraceM:
  // Hint: Implement a mutable private state variable to hold the flatMap count.
  def resetFlatMapCount(): Unit = flatMapCount == 0
  def getFlatMapCount: Int =  flatMapCount
  def incFlatMapCount(): Unit = _

/**
 * The code for TraceM being a Monad instance. This is standard, similar to the
 * one for Option and List.
 */
given traceMIsAMonad: Monad[TraceM] = new Monad[TraceM]:
  def pure[A](a: A): TraceM[A] = TraceM(a)
  def flatMap[A, B](ma: TraceM[A])(f: A => TraceM[B]): TraceM[B] = ma.flatMap(f)
  def withFilter[A](ma: TraceM[A])(f: A => Boolean): TraceM[A] = ma.withFilter(f)

/**
 * Here we have some usage examples. Watch the video to see what is supposed to happen
 * when you have a correct solution. Your job will be to replicate the behavior in the
 * video by implementing a correct solution for TraceM
 */
@main
def main(): Unit =
  def gcdPrint[F[_]: Monad](x: Int, y: Int): Unit =
    println(s"Greatest common divisor of (${x}, ${y}): ${greatestCommonDivisor(summon[Monad[F]].pure((x, y)))}")


  var flatMapCount=0
  gcdPrint[Option](144, 128)
  gcdPrint[Option](12345, 111111110)
  gcdPrint[List](654321, 987654)
  gcdPrint[List](9876543, 1234567)


  TraceM.resetFlatMapCount()
  gcdPrint[TraceM](144, 128)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times")  // 6 times

  TraceM.resetFlatMapCount()
  gcdPrint[TraceM](12345, 111111110)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 18 times

  TraceM.resetFlatMapCount()
  gcdPrint[TraceM](654321777, 987654333)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 34 times

  TraceM.resetFlatMapCount()
  gcdPrint[TraceM](9876543, 1234567)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 14 times

  def lcmPrint[F[_] : Monad](x: Int, y: Int): Unit =
    println(s"Least common multiple of (${x}, ${y}): ${leastCommonMultiple(summon[Monad[F]].pure((x, y)))}")

  lcmPrint[Option](33, 55)
  lcmPrint[Option](111, 198)
  lcmPrint[List](49, 91)
  lcmPrint[List](505, 625)

  TraceM.resetFlatMapCount()
  lcmPrint[TraceM](33, 55)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 11 times

  TraceM.resetFlatMapCount()
  lcmPrint[TraceM](111, 198)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 113 times

  TraceM.resetFlatMapCount()
  lcmPrint[TraceM](49, 91)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 23 times

  TraceM.resetFlatMapCount()
  lcmPrint[TraceM](505, 625)
  println(s"flatMap invoked ${TraceM.getFlatMapCount} times") // 305 times
