package com.nuctech.test

import scala.actors.Actor

/**
 * Created by shangjianping on 2016/4/22.
 */
class ActorTest(val balance: Integer) extends Actor {
  def act() {
    react {
      case _ =>println(balance+":ok"); exit
    }
  }
}

object ActorTest{
  def main(args: Array[String]){
    var test=""
    val aa="adsaa"
   val a = new Thread(new Runnable {
     override def run(): Unit ={
       //Thread.sleep(2000)
       println(Thread.currentThread().getId+":a ok")
     }
   })
    val b =  new Thread(new Runnable {
      override def run(): Unit ={
        println(Thread.currentThread().getId+":b ok"+ aa)
        test=Thread.currentThread().getId+":b ok"
      }
    })
    val c = new Thread(new Runnable {
      override def run(): Unit ={
        //Thread.sleep(1000);
        println(Thread.currentThread().getId+":c ok")
      }
    })
    val d:Array[Thread] = Array(a,b,c)

    a.start()
    b.start()
    c.start()
    for(thread <- d){
      thread.join()
    }
    println(Thread.currentThread().getId+":ok")
    print(test)
  }
}
