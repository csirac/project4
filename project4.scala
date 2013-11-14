import akka.actor._
import akka.routing.RoundRobinRouter
import akka.util.Timeout
import akka.pattern.ask
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.math
import scala.util.Random
import scala.concurrent.Await
import scala.concurrent.Future
import scala.collection.mutable.ArrayBuffer
import java.io._

case class Begin(a: ActorRef, b: String, c: String, f: BufferedWriter )
case class PSBegin
case class PSSend(a: Double, b:Double)
case class PSSet(a: Double, b:Double)
case class Rumor
case class Response(r: Double = 0)
case class Neighbor(a: ActorRef)
case class ANeighbor(a: ArrayBuffer[ActorRef])
case class ReadyQuery
case class PSQuery
case class ReadyResponse
case class SetActors(b: ArrayBuffer[ActorRef] )
case class ShutDown
case class TimeQuery
case class CompletionQuery
case class TurnOff
case class TurnOn
case class Reset

class Node(listener : ActorRef, id : Int) extends Actor { 
//    val r = new Random();
  var offline : Boolean = false;
  var intheloop : Int = 0;
  var neighbor : ArrayBuffer[ActorRef] = ArrayBuffer()

  // Push-Sum variables
  var s : Double = 0
  var w : Double = 0
  var ns : Double = 0
  var nw : Double = 0
  var delta : Double = 0
  val dconst : Double = .0000000001
  var ccounter : Int = 0;

    def receive = { 
      case Reset => {

	intheloop = 0;
	s = id;
	w = 1.0;
	ccounter = 0;
	ns = 0
	nw = 0

	sender ! true
      }
      case TurnOff => {
	offline = true;

	sender ! true
      }
      case TurnOn => {
	offline = false;
	
	sender ! true
      }
      case PSBegin => {
	if (!offline) {
	  var i : Int = Random.nextInt(neighbor.length);
	  neighbor(i) !  PSSend( s / 2, w / 2);
	  s = s/2;
	  w = w/2;
	}
      }
      case PSSet(a: Double, b: Double) => {
	s = a;
	w = b;
      }

      case PSSend(a: Double, b: Double) => {
	if (!offline) {
	  ns = s + a;
	  nw = w + b;
	  delta = s / w - ns / nw;
	  s = ns;
	  w = nw;
	  delta = delta.abs;

	  if (delta < dconst) {
	    ccounter += 1;
	  } else
	    ccounter = 0;

	  if (ccounter < 3) {
	    //actively continue calculation
	    var i : Int = Random.nextInt(neighbor.length);
	    neighbor(i) !  PSSend( s / 2, w / 2);
	    s = s/2;
	    w = w/2;
	  } else {
	    //node has converged. Continue calculation anyway, but notify listener
	    // the first time
	    intheloop += 1;
	    if (intheloop == 1)
	      listener ! Response( s / w );
	    var i : Int = Random.nextInt(neighbor.length);
	    neighbor(i) !  PSSend( s / 2, w / 2);
	    s = s/2;
	    w = w/2;
	  }
	}
      }
      case Rumor => {
	
	if (!offline) {
	  intheloop += 1;
	  if (intheloop == 1) {
	    listener ! Response(0);
	  }

	  var i : Int = Random.nextInt(neighbor.length);
	  neighbor(i) ! Rumor
	}
      }
      
      case Neighbor( a ) => {
	if (!(neighbor.contains(a)))
	  neighbor += a ;
      }
      case ANeighbor( a ) => {
	neighbor = a;
      }
      case ReadyQuery => {
	if (!offline) {
	  sender ! true
	} else
	  sender ! false
      }
      
    }
}
class Listener(n : Int) extends Actor {
  var actor : ArrayBuffer[ActorRef] = null
  var c : Int = 0
  var readycounter : Int = 0
  var starttime : Long = 0;
  var endtime : Long = 0;
  var bw : BufferedWriter = null
  var topology : String = null
  var algorithm : String = null
  def receive = {
    case Reset => {
      	for (i <- 0 to (n-1)) {
	  implicit val timeout = Timeout(20 seconds)
	  var isready: Boolean = false;
	  val future = actor(i) ? TurnOff
	  isready =  Await.result(future.mapTo[Boolean], timeout.duration )
	  if (!isready)
	    sender ! false
	}

      //reset all nodes
      	for (i <- 0 to (n-1)) {
	  implicit val timeout = Timeout(20 seconds)
	  var isready: Boolean = false;
	  val future = actor(i) ? Reset
	  isready =  Await.result(future.mapTo[Boolean], timeout.duration )
	  if (!isready)
	    sender ! false
	}


      //All nodes are now off.
      	for (i <- 0 to (n-1)) {
	  implicit val timeout = Timeout(20 seconds)
	  var isready: Boolean = false;
	  val future = actor(i) ? TurnOn
	  isready =  Await.result(future.mapTo[Boolean], timeout.duration )
	  if (!isready)
	    sender ! false
	}

      //All nodes are back on and reset.
      for (i <- 0 to (n-1)) {
	  implicit val timeout = Timeout(20 seconds)
	  var isready: Boolean = false;
	  val future = actor(i) ? ReadyQuery
	  isready =  Await.result(future.mapTo[Boolean], timeout.duration )
	  if (!isready)
	    sender ! false
	}
      //All nodes are ready.
	sender ! true
    }
    case ShutDown => {
      context.system.shutdown
    }
      case CompletionQuery => {
	if (c < n)
	  sender ! false
	else {
	  sender ! true
	  c = 0
	}
      }
      case TimeQuery => {
	sender ! endtime
      }
      case Begin(a, b, alg, d) => {
	starttime = System.currentTimeMillis;
	topology = b
	algorithm = alg
	bw = d
	if (algorithm == "GOSSIP")
	  a ! Rumor
	else
	  a ! PSBegin
      }
      case Response(result) => {
	c += 1
	//println(i);
	statusbar(c - 1,n);
	if (c == n) {
	  endtime = System.currentTimeMillis - starttime;
	  print("\n");
	  var out : String = n.toString + " " + endtime.toString + "\n"; 

//	  println( out );
//	  bw.write( out );
//	  bw.close();
//	  context.system.shutdown
	}
      }
    
      case ReadyQuery => {
	
	for (i <- 0 to (n-1)) {
	  implicit val timeout = Timeout(20 seconds)
	  var isready: Boolean = false;
	  val future = actor(i) ? ReadyQuery
	  isready =  Await.result(future.mapTo[Boolean], timeout.duration )
	  if (!isready)
	    sender ! false
	}
	sender ! true
      }
      case ReadyResponse => {
	readycounter += 1

      }
      case SetActors(actors) => {
	actor = actors;
      }

    }
  def statusbar( i: Int, n: Int ) {
    var k = i + 1;
    var progress :Double = k.toDouble / n * 65;
    var pprogress :Double = (k - 1).toDouble / n * 65;
    var iprogress = progress.toInt;
    var ipprogress = pprogress.toInt;
    if (iprogress > ipprogress) {
     print("\r|");
    
     for (j <- 0 to 64) {
       if (j <= progress.toInt) {
	 print("=")
       } else {
	 print(" ")
       }
       
     }
      printf("| %.1f ", k.toDouble / n * 100) ; print("%");
    }
  }
   
  }

object project4 {
  def main( args: Array[String] ) {
    var n = args(0).toInt;
    
    var topology = args(1).toString;
    topology = topology.toUpperCase
    
    var algorithm = args(2).toString;
    algorithm = algorithm.toUpperCase

    var timearray : ArrayBuffer[Long] = ArrayBuffer();

    var k : Int = 0;
    val navg : Int = 1;
    var fname : String = algorithm + topology + "_out.txt";
    var file : File =new File( fname );
    if(!file.exists()){
      file.createNewFile();
    }
    var fw :FileWriter = new FileWriter(file.getName(),true);
    var bw : BufferedWriter = new BufferedWriter(fw);
    var sname : String = "system";
    val system = ActorSystem(sname);
    val (actor : ArrayBuffer[ActorRef], listener : ActorRef ) = buildtopology( system, n, topology);

    for (k <- 1 to navg) {

      //Output to file

      val j = Random.nextInt(n);
      //wait for system to be ready
      println("Checking that system is ready...")
      implicit val timeout = Timeout(1 minutes)
      var isready: Boolean = false;
      var isfinished : Boolean = false;
      var timetaken : Long = 0;
      val future0 = listener ? ReadyQuery
      isready =  Await.result(future0.mapTo[Boolean] , timeout.duration )
      if (isready == true) {
	printf("Starting %s protocol...\n", algorithm);
	listener ! Begin(actor(j), topology, algorithm, bw)
	while (isfinished == false) {

	    Thread.sleep(1000);
	  val future = listener ? CompletionQuery
	  isfinished =  Await.result(future.mapTo[Boolean] , timeout.duration )
	}

	//Okay, the actor system is finished with the calculation.
	//Get the time taken from the listener
	val future1 = listener ? TimeQuery
	timetaken =  Await.result(future1.mapTo[Long] , timeout.duration )
	timearray += timetaken ;
	printf("Time taken: %d\n", timetaken);
	//Tell the listener to reset the actor system
	isfinished = false;
	println("Resetting...");
	val future2 = listener ? Reset
	isfinished =  Await.result(future2.mapTo[Boolean] , timeout.duration )

      } else {
	println("Error during initialization.")
      }
      
    }
    //Finished run. Let's shutdown the system.
    println("Shutting down system...");
    listener ! ShutDown
    //Okay, we have a bunch of times
    //Compute mean and stddev
    var mu : Double = 0;
    var stddev : Double = 0;
    for (k <- 0 to (navg - 1)) {
      mu += timearray( k )
    }
    mu = mu / navg;

    for (k <- 0 to (navg - 1)) {
      stddev += (timearray(k) - mu)*(timearray(k) - mu);
    }
    stddev = stddev / navg;
    stddev = math.sqrt( stddev );

    //Output the results to file
    var strout : String = n.toString + " " + mu.toString + " " + stddev.toString + "\n";
    println( strout );
    bw.write( strout );
    bw.close();

  }

  def setupsys( n : Int, system : ActorSystem ) = {
    var actor : ArrayBuffer[ActorRef] = ArrayBuffer();
    val listener = system.actorOf(Props(classOf[Listener],n), name = "listener");
    var i = 0
    for (i <- 0 to (n - 1)) {
      statusbar(i, n)
      var wname = "node" + i.toString;
      val node = system.actorOf(Props(classOf[Node], listener, (i + 1)), name = wname);
      actor += node ;
    }
    listener ! SetActors( actor );
    print("\n");

    (actor, listener);
  }
  def index( row: Int, col: Int, m: Int ) = {
    m*row + col;
  }
  def statusbar( i: Int, n: Int ) {
    var k = i + 1;
    var progress :Double = k.toDouble / n * 65;
    var pprogress :Double = (k - 1).toDouble / n * 65;
    var iprogress = progress.toInt;
    var ipprogress = pprogress.toInt;
    if (iprogress > ipprogress) {
     print("\r|");
    
     for (j <- 0 to 64) {
       if (j <= progress.toInt) {
	 print("=")
       } else {
	 print(" ")
       }
       
     }
      printf("| %.1f ", k.toDouble / n * 100); print("%");
    }
  } 

def buildtopology( system : ActorSystem, nn : Int, topology : String ) = {
  var n = nn;
  var actor : ArrayBuffer[ActorRef] = ArrayBuffer()
  var listener : ActorRef = null


    topology match {
      case "LINE" =>
	println("Setting up actor system...");
	val (a,b) = setupsys( n, system );
	actor = a
	listener = b
	printf("Setting up %s topology...\n", topology);
	for (i <- 0 to (n - 1)) {
	  statusbar(i, n);
	  actor(i) ! PSSet( i.toDouble + 1, 1.0 );
	  if (i == 0) {
	    actor.apply(i) ! Neighbor( actor.apply(1) )
	  } else {
	    if (i == (n-1)) {
	      actor.apply(i) ! Neighbor( actor.apply(n - 2) )
	    } else {
	      actor.apply(i) ! Neighbor( actor.apply(i - 1) )
	      actor.apply(i) ! Neighbor( actor.apply(i + 1) )
	    }
	  }
	}
      print("\n");
      case "FULL" => {
	println("Setting up actor system...")
	val (a,b) = setupsys( n, system );
	printf("Setting up %s topology...\n", topology);

	actor = a
	listener = b
	for (i <- 0 to (n-1)) {
	  statusbar(i, n);
	  actor(i) ! PSSet( i.toDouble + 1, 1.0);
	  var nbh : ArrayBuffer[ActorRef] = actor.to;
	  nbh.remove(i);
	  actor(i) ! ANeighbor(nbh);
	  
	}
	print("\n");
      }
      case "2D" => {

	var m = math.ceil(math.sqrt(n)).toInt;
	n = m * m;
	var r1 : Int = 0
	var r2 : Int = 0
	var c1 : Int = 0
	var c2 : Int = 0
	println("Setting up actor system...");
	val (a,b) = setupsys( n, system );
	actor = a
	listener = b
	printf("Setting up %s topology...\n", topology);
	for (i <- 0 to (m - 1)) {
	  for (j <- 0 to (m - 1)) {
	    statusbar( index(i,j,m ), n);
	    actor( index(i,j,m)) ! PSSet( index(i,j,m) + 1, 1.0 );
	    r1 = i + 1;
	    r2 = i - 1;
	    c1 = j + 1;
	    c2 = j - 1;
	    if (i == 0) 
	      r2 = r1;
	    if (i == (m - 1))
	      r1 = r2;
	    if (j == 0)
	      c2 = c1;
	    if (j == (m - 1))
	      c1 = c2;

	    actor(index(i, j, m)) ! Neighbor(actor( index(r1, j, m ) ) )
	    actor(index(i, j, m)) ! Neighbor(actor( index(r2, j, m ) ) )
	    actor(index(i, j, m)) ! Neighbor(actor( index(i, c1, m ) ) )	    
	    actor(index(i, j, m)) ! Neighbor(actor( index(i, c2, m ) ) )	    
	  }
	  
	}

	print("\n")
	
      }
      case "IMP2D" => {
	var m = math.ceil(math.sqrt(n)).toInt;
	n = m * m;
	var r1 : Int = 0
	var r2 : Int = 0
	var c1 : Int = 0
	var c2 : Int = 0
	println("Setting up actor system...");
	val (a,b) = setupsys( n, system );
	actor = a
	listener = b
	printf("Setting up %s topology...\n", topology);
	for (i <- 0 to (m - 1)) {
	  for (j <- 0 to (m - 1)) {
	    statusbar( index(i,j,m), n);
	    actor( index(i,j,m)) ! PSSet( index(i,j,m) + 1, 1.0 );
	    r1 = i + 1;
	    r2 = i - 1;
	    c1 = j + 1;
	    c2 = j - 1;
	    if (i == 0) 
	      r2 = r1;
	    if (i == (m - 1))
	      r1 = r2;
	    if (j == 0)
	      c2 = c1;
	    if (j == (m - 1))
	      c1 = c2;

	    actor(index(i, j, m)) ! Neighbor(actor( index(r1, j, m ) ) )
	    actor(index(i, j, m)) ! Neighbor(actor( index(r2, j, m ) ) )
	    actor(index(i, j, m)) ! Neighbor(actor( index(i, c1, m ) ) )	    
	    actor(index(i, j, m)) ! Neighbor(actor( index(i, c2, m ) ) )	    

	    actor(index(i, j, m)) ! Neighbor(actor(Random.nextInt(n) ) )
	  }
	  
	}
	print("\n")
     }
    }
  (actor, listener);
}
}


