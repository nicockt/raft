package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	// Goal: send GRPC to other nodes
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	log []*raft.LogEntry
	// TODO: Implement this!

	id 					int32 //node id
	electionTimeout 	int32 //election timeout for this node
	heartBeatInterval 	int32 //heart beat interval for this node

	//Persistent state on all servers
	currentTerm 		int32 //latest term server has seen (default: 0)
	votedFor    		int32 //candidateId that received vote in current term (or null if none)
	kvMap 				map[string]string //key-value map

	//Volatile state on all servers
	commitIndex 		int32 //index of highest log entry known to be committed (default: 0)
	currentLeader 		int32 //id of the leader
	serverState 		raft.Role //0: follower, 1: candidate, 2: leader. role in proto

	//Volatile state on leaders
	nextIndex 			[]int32 //for each server, index of the next log entry to send to that server (default: leader last log index + 1)
	matchIndex 			[]int32 //for each server, index of highest log entry known to be replicated on server (default: 0)

	resetChan chan bool
	finishChan chan bool
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		log: nil,

		id: int32(nodeId),		
		electionTimeout: int32(electionTimeout),
		heartBeatInterval: int32(heartBeatInterval),

		currentTerm: 0, 	// The first leader will increment to 1
		votedFor: -1,		// -1: haven't vote for anyone
		currentLeader: -1,	// -1: no leader
		serverState: raft.Role_Follower,	// Start as Follower
		
		commitIndex: 0,
		nextIndex: make([]int32, 0),
		matchIndex: make([]int32, 0),
		
    	resetChan: make(chan bool),
		finishChan: make(chan bool),
	}


	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect each pair of raft nodes (GRPC)
	// Client: Sender, Host: Receiver
	//Can use this nodeifPortMap to send GRPC to other nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	//TODO: kick off leader election here !
	// Run concurrent goroutine
	ctx := context.Background()
	go func(){
		//infinite loop to check the state of the node
		for{
			switch rn.serverState{
				case raft.Role_Follower:
					// Implement timer with the length of electionTimeOut
					// if timeout, change to candidate
					// If receive msg from other nodes, reset the timer
					// If receive AppendEntries from leader, change to follower
					
					select {
						// Timeout, change to candidate
						case <- time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
							rn.serverState = raft.Role_Candidate
							fmt.Println("Change follower state to candidate")
						// Receive reset signal, reset the timer (i.e. receive msg / setElectionTimeout)
						case <- rn.resetChan:
							// Go back to the begainning of the for loop
							fmt.Println("Follower reset election timeout")
					}


				case raft.Role_Candidate:
					rn.currentTerm++
					rn.votedFor = int32(rn.id)
					voteNum := 0

					// Send out RequestVote GRPC to all other nodes
					// If sequential execute, client dies may block the whole loop
					for hostId, client := range hostConnectionMap {
						go func(hostId int32, client raft.RaftNodeClient){
							// variable r to receive the result of the RequestVote GRPC
							r, err:= client.RequestVote(ctx, &raft.RequestVoteArgs{
								From: int32(rn.id),
								To: int32(hostId),
								Term: rn.currentTerm,
								CandidateId: int32(rn.id),
								LastLogIndex: int32(0), //TODO: update this
								LastLogTerm: int32(0), //TODO: update this
							})
							if err != nil && r.VoteGranted == true && r.Term == rn.currentTerm{ 
								// Race condition: multiple goroutines may update the voteNum at the same time
								//lock
								voteNum++
								//unlock

								// If majority, change to leader
								// hostConnectionMap is all the other nodes, except itself
								// so voteNum == len(hostConnectionMap)/2 means majority
								if voteNum >= len(hostConnectionMap)/2 && rn.serverState == raft.Role_Candidate{
									rn.serverState = raft.Role_Leader
									fmt.Println("Change candidate state to leader")
									rn.finishChan <- true //Leave the Candidate state, Back to the begainning of the outer for loop
								}
							}else if r.Term > rn.currentTerm{
								
							}
						}(hostId, client)
					}

					
					// Keep track of the votes. If majority, change to leader
					select {
						case <- time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
							// If timeout, restart the election
							fmt.Println("Candidate restart election")
						case <- rn.resetChan:
							// Go back to the begainning of the for loop
							fmt.Println("Candidate reset election timeout")
						case <- rn.finishChan:
							// Go back to the begainning of the for loop
							fmt.Println("Candidate finished election timeout")

					}

				case raft.Role_Leader:
					// Replicate Log: Send out appendEntires GRPC to all followers
					// Send different log entry to different followers according to commitIndex & mathcIndex
					select{}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
// Log replication
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!
	log.Printf("Receive propose from client")
	var ret raft.ProposeReply

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!
	var ret raft.GetValueReply
	//map key to value
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
// Leader Election
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this!
	var reply raft.RequestVoteReply
	reply.From = args.To
	reply.To = args.From

	// Handle if args.Term > rn.currentTerm

	// If the candidate's term is less than the current term, reject the vote
	// If the candidate's term is greater than the current term, vote for the candidate
	if args.Term >= rn.currentTerm && (rn.votedFor == -1 || rn.votedFor == args.CandidateId) && (true){
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
	}else{
		reply.VoteGranted = false
	}

	if reply.VoteGranted == true{
		// reset the eclection timeout
		rn.resetChan <- true
	}
	

	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this
	var reply raft.AppendEntriesReply
	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!
	var reply raft.SetElectionTimeoutReply
	fmt.Println("Set election timeout")
	rn.electionTimeout = args.Timeout // update electionTimeout
	rn.resetChan <- true // reset electionTicker
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	fmt.Println("Set heartBeat interval")
	rn.heartBeatInterval = args.Interval // update heartBeatInterval
	rn.resetChan <- true  // reset heartBeatTicker
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
