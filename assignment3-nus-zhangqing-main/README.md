# IT5100A 2023 - Assignment 3

## Write your own Monad

In this assignment you will have to write your own Monad, named `traceM`.

The code provided contains the following:
* The Monad code from the lecture, required for the implementation of any Monad
* The Monad instance code for Option and List, to play around for
* Two monadic examples, simulating imperative programming
  * Greatest Common Divisor (GCD)
  * Least Common Multiple (LCM)
  * These are still PURE computations, but offer the illusion of imperative programming
  * For instance, the variables appear to be assigned, but they in fact are not.

A way to evaluate the performance of the program is to count the
number of computation steps it performs. So we want to define our own
monad which, when injected into the GCD and LCM computations, can report
back the number of steps (i.e. the number of invocations of `flatMap`)
performed throughout the computation.

For a better explanation, watch the video provided.

The task of this assignment is to fill into the code template provided to
implement the Monad that will make the tests pass.