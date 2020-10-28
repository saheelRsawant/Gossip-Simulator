#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open Akka.Actor
open Akka.FSharp
open System


type Message = 
    | MessageDeliveredGossip of string
    | MessageDeliveredPushSum of double * double
    | BeginStopwatch
    | NumberOfActors of int 
    | CreateTopology of IActorRef []
    | StartRumour of string
    | StartPushSum of double * double * double

let system = ActorSystem.Create("DOSProject2")
let random = System.Random(0)

let args : string array = fsi.CommandLineArgs |> Array.tail
let mutable numNodes =  args.[0] |> int
let topology = args.[1] |> string
let algorithm =  args.[2] |> string

let msgLimit = 10


let BossActor (mailbox:Actor<_>) = 
    let mutable getStartTime = System.Diagnostics.Stopwatch.StartNew() 
    let mutable getNumberOfActors = 0
    let mutable counter = 0 

    let rec loop() = actor {
            let! message = mailbox.Receive()
            match message with
                | MessageDeliveredGossip(str)->
                    counter <- counter + 1
                    if counter = getNumberOfActors then
                        printfn "Gossip Convergence time is %f milliseconds" (getStartTime.Elapsed.TotalMilliseconds)
                        Environment.Exit 0

                |  MessageDeliveredPushSum(sum,weight) -> 
                    printfn "Sum is %f \n" sum 
                    printfn "Weight is %f \n" weight 
                    printfn "Average is %f \n" (sum/weight)
                    printfn "PushSum Convergence time is %f milliseconds" (getStartTime.Elapsed.TotalMilliseconds)
                    Environment.Exit 0
                  
                | BeginStopwatch ->
                    getStartTime <- System.Diagnostics.Stopwatch.StartNew()

                | NumberOfActors(numActors) ->
                    getNumberOfActors <- numActors

                | _-> ()
            return! loop()
        }
    loop()



let ChildActors(nodeNum: int)  (childActorRef : IActorRef)(mailBox:Actor<_>) =
        let mutable msgCounter = 0
        let mutable sum = nodeNum |> double
        let mutable weight = 1.0

        let mutable index = 1
        let mutable neighbours:IActorRef[]=[||] 

        let rec loop() = actor {
            let! message = mailBox.Receive()
            match message with
                | CreateTopology(actorRef)->
                    neighbours <- actorRef
                    
                |  StartRumour(msg) -> 
                    msgCounter<- msgCounter + 1
                    if(msgCounter = msgLimit) then
                        childActorRef <! MessageDeliveredGossip(msg)
                        
                    if (msgCounter < 150) then
                       let randNeighbor = random.Next(0,neighbours.Length)
                       neighbours.[randNeighbor] <! StartRumour(msg)


                | StartPushSum(s:double, w, delta) ->
                    if(s = double(nodeNum) && w = 1.0 && (delta = 10.0 ** -10.0)) then
                        let randNeighbor = random.Next(0,neighbours.Length)
                        sum <- sum /2.0 
                        weight <- weight / 2.0
                        neighbours.[randNeighbor] <! StartPushSum(sum, weight, delta)
                    else
                        let s' = sum + s
                        let w' = weight + w
                        let ans = ((sum/weight) - (s'/w')) |> abs

                        if(ans > delta) then 
                            index <- 0
                            sum <- sum + s
                            weight <- weight + w

                            sum <- sum /2.0
                            weight<- weight / 2.0

                            let randNeighbor = random.Next(0,neighbours.Length)
                            neighbours.[randNeighbor] <! StartPushSum(sum, weight, delta)
                        elif (index >=3) then
                            childActorRef <! MessageDeliveredPushSum(sum,weight)
                        else
                            sum <- sum /2.0
                            weight<- weight / 2.0
                            index <- index + 1

                            let randNeighbor = random.Next(0, neighbours.Length)
                            neighbours.[randNeighbor] <! StartPushSum(sum, weight, delta)  
                | _-> ()
            return! loop()

        }
        loop()


let childActorRef = BossActor |> spawn system "childActorRef"



// This block contains gossip and pushsum algorithms for FULL topology
if topology = "full" then
    let fullArr = Array.zeroCreate(numNodes)
    for a in 0..numNodes-1 do
        fullArr.[a] <- ChildActors (a+1) childActorRef|> spawn system ("ChildActor" + string(a))

    for b in 0..numNodes-1 do 
        fullArr.[b] <! CreateTopology(fullArr)

    let fullRandNeighbor = random.Next(0,numNodes)
    
    if algorithm = "gossip" then
        childActorRef <! NumberOfActors(numNodes)
        childActorRef <! BeginStopwatch
        printfn "Gossip algorithm for full topology :\n"
        fullArr.[fullRandNeighbor]<! StartRumour("Hi")
    elif algorithm = "push-sum" then
        childActorRef <! BeginStopwatch
        printfn "Pushsum algorithm for full topology :\n"
        fullArr.[fullRandNeighbor]<! StartPushSum(double(numNodes), 1.0, (10.0 ** -10.0))
    else 
        printfn "Invalid Algorithm"



// This block contains gossip and pushsum algorithms for LINE topology
if topology = "line" then
    let lineArr = Array.zeroCreate(numNodes)

    for i in 0..numNodes-1 do
        lineArr.[i] <- ChildActors (i+1)  childActorRef |> spawn system ("ChildActor" + string(i))

    let finalList = lineArr |> Array.toList
    let mutable  neighbours:IActorRef[]=[||]
    let mutable nei: IActorRef list = []

    [ 0 .. numNodes-1]   
        |> List.iter (fun x ->
            if x = 0 then
                nei <- [ finalList.[1] ] |> List.append nei
            elif x = numNodes-1 then 
                nei <- [ finalList.[numNodes-2] ] |> List.append nei
            else 
                nei <- [ finalList.[x-1]; finalList.[x+1] ] |> List.append nei
    )
    for i in [0..numNodes-1] do    
        neighbours<-nei |> List.toArray
        lineArr.[i]<!CreateTopology(neighbours)  

    let lineRandNeighbor = random.Next(0,numNodes)

    if algorithm = "gossip" then
        childActorRef<!NumberOfActors(numNodes)
        childActorRef<!BeginStopwatch
        printfn "Gossip algorithm for line topology :\n"
        lineArr.[lineRandNeighbor]<!StartRumour("Hi")
    elif algorithm="push-sum" then
        childActorRef <! BeginStopwatch
        printfn "Pushsum algorithm for line topology :\n"
        lineArr.[lineRandNeighbor]<!StartPushSum(double(numNodes), 1.0, (10.0 ** -10.0))   
    else
        printfn "Invalid Algorithm"



// This block contains gossip and pushsum algorithms for 2D topology
if topology = "2D" then
    let size = numNodes |> float |> sqrt |> ceil |> int
    let gridSize = pown size 2
    
    let twoDArr = Array.zeroCreate(gridSize)
    let limit = gridSize - 1
    
    for i in [0..limit] do
        twoDArr.[i]<-ChildActors (i+1)  childActorRef |> spawn system ("ChildActor" + string(i))

    for i in [0..size-1] do
        for j in [0..size-1] do
            let mutable llist:IActorRef[]=[||]
            
            if j+1 < size then
                llist <- Array.append llist [|twoDArr.[i*size+j+1]|]
            if j-1 >= 0 then
                llist <- Array.append llist [|twoDArr.[i*size+j-1]|]
            if i-1 >= 0 then
                llist <- Array.append llist [|twoDArr.[(i-1)*size+j]|]
            if i+1 < size then
                llist <- Array.append llist [|twoDArr.[(i+1)*size+j]|]
            twoDArr.[i*size+j]<!CreateTopology(llist)

    let randNeighbor = System.Random().Next(0,gridSize - 1)
    
    if algorithm = "gossip" then
       childActorRef<!NumberOfActors(gridSize - 1)
       childActorRef<!BeginStopwatch
       printfn "Gossip algorithm for 2D topology :\n"
       twoDArr.[randNeighbor]<!StartRumour("Hello World")
    else if algorithm="push-sum" then
       childActorRef<!BeginStopwatch
       printfn "Pushsum algorithm for 2D topology :\n"
       twoDArr.[randNeighbor]<!StartPushSum(double(numNodes), 1.0, (10.0 ** -10.0))



// This block contains gossip and pushsum algorithms for imp2D topology
if topology = "imp2D" then
    let size = numNodes |> float |> sqrt |> ceil |> int
    let gridSizeImp2D = pown size 2
   
    let imp2DArr = Array.zeroCreate(gridSizeImp2D)
    let limit = gridSizeImp2D - 1

    for i in [0..limit] do
       imp2DArr.[i]<-ChildActors (i+1)  childActorRef |> spawn system ("ChildActor" + string(i))
   
    for m in [0..size-1] do
        for n in [0..size-1] do
            let mutable llist:IActorRef[]=[||]
            
            if n+1 < size then
                llist<- Array.append llist [|imp2DArr.[m*size+n+1]|]
            if n-1 >= 0 then
                llist<-Array.append llist [|imp2DArr.[m*size+n-1]|]
            if m-1 >= 0 then
                llist <-Array.append llist [|imp2DArr.[(m-1)*size+n]|]
            if m+1 < size then
                llist <- Array.append llist [|imp2DArr.[(m+1)*size+n]|]
           
            let randNeighImp2D = System.Random().Next(0, gridSizeImp2D - 1) 
            llist <- (Array.append llist [|imp2DArr.[randNeighImp2D]|])
            
            imp2DArr.[m*size+n]<!CreateTopology(llist)

    let randNeighbor = System.Random().Next(0,gridSizeImp2D - 1) 

    if algorithm = "gossip" then
        childActorRef<!NumberOfActors(gridSizeImp2D - 1)
        childActorRef<!BeginStopwatch
        printfn "Gossip algorithm for imp2D topology :\n"
        imp2DArr.[randNeighbor]<!StartRumour("Hello World")
    else if algorithm="push-sum" then
        childActorRef<!BeginStopwatch
        printfn "Pushsum algorithm for imp2D topology :\n"
        imp2DArr.[randNeighbor]<!StartPushSum(double(numNodes), 1.0, (10.0 ** -10.0))

  

System.Console.ReadLine() |> ignore


