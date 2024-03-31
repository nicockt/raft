package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
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
	log []*raft.LogEntry // log starts from 1
	// TODO: Implement this!

	id 					int32 //node id
	electionTimeout 	int32 //election timeout for this node
	heartBeatInterval 	int32 //heart beat interval for this node

	//Persistent state on all servers
	currentTerm 		int32 //latest term server has seen (default: 0)
	votedFor    		int32 //candidateId that received vote in current term (or null if none)
	kvMap 				map[string]int32 //key-value map

	//Volatile state on all servers
	commitIndex 		int32 //index of highest log entry known to be committed (default: 0)
	currentLeader 		int32 //id of the leader
	serverState 		raft.Role //0: follower, 1: candidate, 2: leader. role in proto

	//Volatile state on leaders, TODO: map[int32]int32 ?
	nextIndex 			[]int32 //for each server, index of the next log entry to send to that server (default: leader last log index + 1)
	matchIndex 			[]int32 //for each server, index of highest log entry known to be replicated on server (default: 0)

	resetChan chan bool
	finishChan chan bool
	heartbeatChan chan bool
	commitChan chan bool
	mu sync.Mutex
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
		log: []*raft.LogEntry{nil},	// log starts from 1

		id: int32(nodeId),		
		electionTimeout: int32(electionTimeout),
		heartBeatInterval: int32(heartBeatInterval),

		currentTerm: 0, 	// The first leader will increment to 1
		votedFor: -1,		// -1: haven't vote for anyone
		currentLeader: -1,	// -1: no leader
		serverState: raft.Role_Follower,	// Start as Follower
		
		commitIndex: 0,  // To be updated when log is committed
		
    	resetChan: make(chan bool),
		heartbeatChan: make(chan bool),
		finishChan: make(chan bool),
		commitChan: make(chan bool),
		mu: sync.Mutex{},
		kvMap: make(map[string]int32),

		nextIndex: nil,
		matchIndex: nil,
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
	go func(){
		//infinite loop to check the state of the node
		initial := true
		for{
			switch rn.serverState{
				case raft.Role_Follower:
					// Implement timer with the length of electionTimeOut
					// if timeout, change to candidate
					// If receive msg from other nodes, reset the timer

					select {
						// Set timer to electionTimeout. If timeout, change to candidate
						case <- time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
							rn.serverState = raft.Role_Candidate
							log.Println("Change follower state to candidate, id: ", rn.id)
						
						case <- rn.heartbeatChan:
							// Reset when received heartbeat from leader
						
						case <- rn.resetChan:
							// Reset electionTimeout, Go back to the begainning of the for loop
					}


				case raft.Role_Candidate:
					rn.currentTerm++
					rn.votedFor = int32(rn.id)
					var lastLogIndex int32 = 0
					var lastLogTerm int32 = 0
					if len(rn.log) > 1{
						lastLogIndex = int32(len(rn.log) - 1) // lastLogIndex starts from 1 (if log is not empty)
						lastLogTerm = rn.log[lastLogIndex].Term
					}
					voteNum := 0

					// Send out RequestVote GRPC to all other nodes
					// If sequential execute, client dies may block the whole loop
					for hostId, client := range hostConnectionMap {
						go func(hostId int32, client raft.RaftNodeClient){
							// 100 ms timeout for follower communication
							ctx, cancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
							defer cancel()
							// variable r to receive the result of the RequestVote GRPC
							r, err:= client.RequestVote(ctx, &raft.RequestVoteArgs{
								From: int32(rn.id),
								To: int32(hostId),
								Term: rn.currentTerm,
								CandidateId: int32(rn.id),
								LastLogIndex: lastLogIndex,  
								LastLogTerm: lastLogTerm, 
							})

							if err == nil && r.VoteGranted && r.Term == rn.currentTerm{ 
								// Race condition: multiple goroutines may update the voteNum at the same time
								rn.mu.Lock() // Write lock
								voteNum++
								rn.mu.Unlock() //unlock

								// If majority, change to leader
								// hostConnectionMap is all the other nodes, except itself
								// The node votes for itself, so half of hostConnectionMap voteNum == len(hostConnectionMap)/2 means majority
								if voteNum >= len(hostConnectionMap)/2 && rn.serverState == raft.Role_Candidate{
									rn.serverState = raft.Role_Leader
									log.Println("Change candidate state to leader")
									rn.finishChan <- true //Leave the Candidate state, Back to the begainning of the outer for loop
								}
							}else if err == nil && r.Term > rn.currentTerm{ // other node term term > candidate term
								rn.serverState = raft.Role_Follower
								rn.currentTerm = r.Term
								rn.votedFor = -1
								rn.currentLeader = -1
								rn.finishChan <- true
								log.Println("Candidate node",rn.id,"change to follower, outdated leader")
							}
						}(hostId, client)
					}

					// Keep track of the votes. If majority, change to leader
					select {
						// Candidate election timeout, no one wins election
						case <- time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
							log.Println("Candidate restart election, no one wins the election")
						
						// get appendEntries from somebody L else
						case <- rn.resetChan:
							// Go back to the begainning of the for loop
							log.Println("Candidate reset election timeout")
						
						// Candidate finished the election (won / oudated / leader exists)
						case <- rn.finishChan:
							log.Println("Candidate finished the election")

					}

				case raft.Role_Leader:
					// Replicate Log: Send out appendEntires GRPC to all followers
					// Send different log entry to different followers according to commitIndex & mathcIndex
					


					//TODO: update hardcode
					// First heartbeat
					if initial{
						// Initialize the nextIndex and matchIndex with default values
						rn.matchIndex = make([]int32, len(hostConnectionMap) + 1)
						rn.nextIndex = make([]int32, len(hostConnectionMap) + 1)
						// Update nextIndex and matchIndex
						for i := range rn.nextIndex{
							rn.nextIndex[i] = int32(len(rn.log)) // nextIndex starts from 1 (if log is empty)
							rn.matchIndex[i] = 0
						}

						initial = false
						for hostId, client := range hostConnectionMap{
							var prevLogIndex int32 = rn.matchIndex[hostId]
							var prevLogTerm int32 = 0
							if prevLogIndex > 0 && len(rn.log) > 1{ // If the log is not empty
								prevLogTerm = rn.log[prevLogIndex].Term
							}

							go func(hostId int32, client raft.RaftNodeClient){
								ctx, cancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
								defer cancel()
								r, err := client.AppendEntries(ctx, &raft.AppendEntriesArgs{
									From: int32(rn.id),
									To: int32(hostId),
									Term: rn.currentTerm,
									LeaderId: int32(rn.id),
									PrevLogIndex: int32(prevLogIndex),
									PrevLogTerm: prevLogTerm,
									Entries: []*raft.LogEntry{},
									LeaderCommit: rn.commitIndex,
								})

								if err == nil && r.Success { // all followers are up to date
									log.Println("AppendEntries done, from:", r.From, " Success", " Term:", r.Term, " Matchedindex:", r.MatchIndex)

									// Update nextIndex and matchIndex
									if r.MatchIndex > rn.matchIndex[hostId]{
										rn.matchIndex[hostId] = r.MatchIndex
										rn.nextIndex[hostId] = r.MatchIndex + 1
									}

									// Consistency check
									if r.MatchIndex > rn.commitIndex && rn.log[r.MatchIndex].Term == rn.currentTerm && r.MatchIndex <= int32(len(rn.log)){
										//Count how many nodes have committed the log. If majority, leader commit the log
										commitCount := 0
										for _, matchIndex := range rn.matchIndex{
											if matchIndex >= r.MatchIndex{
												commitCount++
											}
										}
										if commitCount >= len(hostConnectionMap)/2{
											rn.commitIndex = r.MatchIndex
											log.Println("Leader commit log")
											rn.commitChan <- true
										}
									}
									
								}else if err == nil && !r.Success && r.Term > rn.currentTerm{ // other node term > leader term
									rn.serverState = raft.Role_Follower
									rn.currentTerm = r.Term
									rn.votedFor = -1
									rn.currentLeader = -1
									log.Println("Leader change to follower, outdated leader")
									rn.finishChan <- true
								}else if err == nil && !r.Success && r.Term <= rn.currentTerm{
									// If the follower's log is outdated, decrement the nextIndex, appendEntries again
									if rn.nextIndex[hostId] >= 1{
										rn.nextIndex[hostId] = rn.nextIndex[hostId] - 1
									}
									log.Println("Leader appendEntries again, outdated log")
								}else{
									//error handling
									log.Println("Error in AppendEntries")
								}
							}(hostId, client)
						}
					}
					
					select{
						case <- time.After(time.Duration(rn.heartBeatInterval) * time.Millisecond):
							for hostId, client := range hostConnectionMap{

								// Get prevLogIndex and prevLogTerm
								var prevLogIndex int32 = rn.matchIndex[hostId] // prevLogIndex starts from 0 if log is empty
								var prevLogTerm int32 = 0
								if prevLogIndex >= 1 && len(rn.log) >= 2{ // If the log is not empty
									prevLogTerm = rn.log[prevLogIndex].Term
								}
								// sendLog depends on the log index of the follower node (host)
								sendLog := []*raft.LogEntry{}
								if int32(len(rn.log)) > prevLogIndex + 1{
									// if the rn.nextIndex has not been updated, send excess log to appendEntries
									sendLog = rn.log[prevLogIndex+1:]
								}

								go func(hostId int32, client raft.RaftNodeClient){
									// 100 ms timeout for follower communication
									ctx, cancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
									defer cancel()

									// variable r to receive the result of the AppendEntries GRPC
									r, err := client.AppendEntries(ctx, &raft.AppendEntriesArgs{
										From: int32(rn.id),
										To: int32(hostId),
										Term: rn.currentTerm,
										LeaderId: int32(rn.id),
										PrevLogIndex: int32(prevLogIndex),
										PrevLogTerm: prevLogTerm,
										Entries: sendLog,
										LeaderCommit: rn.commitIndex,
									})

									if err == nil && r.Success { // all followers are up to date
										log.Println("AppendEntries done, from:", r.From, " Success", " Term:", r.Term, " Matchedindex:", r.MatchIndex)

										// Update nextIndex and matchIndex
										if r.MatchIndex > rn.matchIndex[hostId]{
											rn.matchIndex[hostId] = r.MatchIndex
											rn.nextIndex[hostId] = r.MatchIndex + 1
										}
										log.Println("hostId",hostId,"- matchId[2]: ", rn.matchIndex[2], "matchId[3]]: ", rn.matchIndex[3])
										

										// Consistency check
										// Check all logs from rn.commitIndex + 1 to r.MatchIndex, any to commit?
										for i:= rn.commitIndex + 1; i <= r.MatchIndex; i++{
											if rn.log[r.MatchIndex].Term == rn.currentTerm && r.MatchIndex < int32(len(rn.log)){
												//Count how many nodes have committed the log. If majority, leader commit the log
												commitCount := 0
												for _, matchIndex := range rn.matchIndex{
													log.Println("commit loop check, matchIndex:", matchIndex, "i:", i)
													if matchIndex >= i{
														commitCount++
													}
												}
												if commitCount >= len(hostConnectionMap)/2{
													log.Println("Leader commit log, r.MatchIndex:", i, "rn.commitIndex:", rn.commitIndex, "len(rn.log):", len(rn.log), "commitCount:", commitCount)
													rn.commitChan <- true
												}
											}
										}
										
									}else if err == nil && !r.Success && r.Term > rn.currentTerm{ // other node term > leader term
										rn.serverState = raft.Role_Follower
										rn.currentTerm = r.Term
										rn.votedFor = -1
										rn.currentLeader = -1
										log.Println("Leader change to follower, outdated leader")
										rn.finishChan <- true
									}else if err == nil && !r.Success && r.Term <= rn.currentTerm{
										// If the follower's log is outdated, decrement the nextIndex, appendEntries again
										if rn.nextIndex[hostId] >= 1{
											rn.nextIndex[hostId] = rn.nextIndex[hostId] - 1
										}
										log.Println("Leader appendEntries again, outdated log")
									}else{
										//error handling
										log.Println("Error in AppendEntries")
									}
								}(hostId, client)
							}
						case <- rn.resetChan:
							// Go back to the begainning of the for loop
							log.Println("Leader reset heartBeat interval")

						case <- rn.finishChan:
							log.Println("Leader done, outdated leader")
						
						//TODO: appendEntries reset heartBeatInterval here 
					}
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
// Log replication, client update kvMap
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!
	log.Println("node", rn.id, "receive propose from client")
	var ret raft.ProposeReply

	if rn.serverState == raft.Role_Leader{
		ret.CurrentLeader = rn.votedFor
	}else{  // Proposing to wrong node, not a leader
		log.Println("node", rn.id, "Got Proposed to wrong node")
		ret.CurrentLeader = rn.votedFor
		ret.Status = raft.Status_WrongNode
	}

	if ret.Status != raft.Status_WrongNode{
		rn.log = append(rn.log, &raft.LogEntry{Term: rn.currentTerm, Op: args.Op, Key: args.Key, Value: args.V})
		// Wait until majority of nodes have committed the log	
		log.Println("node", rn.id, "wait for commit, rn.log:",rn.log)
		<- rn.commitChan
		log.Println("node", rn.id, "commit done")

		// Check key exists after commit
		if args.Op == raft.Operation_Delete{
			// Check if the key exists
			// If existed, set ok to true, otherwise false
			if _, ok := rn.kvMap[args.Key]; ok{
				log.Println("node", rn.id, "ok to delete")
				ret.Status = raft.Status_OK
			}else{
				log.Println("node", rn.id, "key not found to delete")
				ret.Status = raft.Status_KeyNotFound

			}
		}else{ // Put a new key-value pair
			log.Println("node", rn.id, "ok to put")
			ret.Status = raft.Status_OK
		}

		// Update kvMap
		rn.mu.Lock()
		if args.Op == raft.Operation_Put{
			log.Println("node", rn.id, "put key-value pair")
			rn.kvMap[args.Key] = args.V
		}else if args.Op == raft.Operation_Delete{
			log.Println("node", rn.id, "delete key-value pair")
			delete(rn.kvMap, args.Key)
		}
		rn.commitIndex++
		log.Println("node", rn.id, " propose done, commitIndex:", rn.commitIndex, "kvMap:", rn.kvMap)
		rn.mu.Unlock()
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
// For grader to get the value
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!
	var ret raft.GetValueReply
	//map key to value
	
	rn.mu.Lock() //Lock
	if val, ok := rn.kvMap[args.Key]; ok{
		ret.V = val
		ret.Status = raft.Status_KeyFound
	}else{
		ret.V = 0
		ret.Status = raft.Status_KeyNotFound
	}
	rn.mu.Unlock() // Unlock
	

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

	// If the candidate's term is less than the current term, reject the vote
	// If the candidate's term is greater than the current term, vote for the candidate
	var lastLogIndex int32 = 0
	var lastLogTerm int32 = 0
	if len(rn.log) > 1{
		lastLogIndex = int32(len(rn.log) - 1) // lastLogIndex starts from 1 (if log is not empty)
		lastLogTerm = rn.log[lastLogIndex].Term
	}

	// Handle if args.Term > rn.currentTerm
	if args.Term > rn.currentTerm{
		rn.votedFor = -1
		rn.currentTerm = args.Term
		if rn.serverState != raft.Role_Follower{
			rn.serverState = raft.Role_Follower
			rn.resetChan <- true
		}
	}
	reply.Term = rn.currentTerm	

	if rn.serverState == raft.Role_Follower && args.Term >= rn.currentTerm && (rn.votedFor == -1 || rn.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex))  {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
		rn.resetChan <- true
	}else{
		reply.VoteGranted = false
	}
	return &reply, nil
}
//TODO: ensure all nodes success?
// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this
	rn.mu.Lock()
	defer rn.mu.Unlock()
	var reply raft.AppendEntriesReply
	reply.From = int32(args.To)
	reply.To = int32(args.From)
	reply.Success = true
	reply.MatchIndex = int32(0) // default value

	// Receive heartbeat from new leader
	if args.Term >= rn.currentTerm{
		rn.votedFor = args.From
		rn.currentLeader = args.LeaderId
		rn.currentTerm = args.Term
		if rn.serverState != raft.Role_Follower{ // if receiver is candidate / leader & sender leader is newer
			// Change to follower
			log.Println("node ", rn.id, " - AppendEntries: ", rn.serverState," change to follower")
			rn.serverState = raft.Role_Follower
			rn.resetChan <- true

		}else{ // if receiver is follower
			// reset the follower's election timeout to avoid timeout
			rn.heartbeatChan <- true
		}
	}

	reply.Term = int32(rn.currentTerm)

	if args.Term < rn.currentTerm{ 
		// leader is outdated
		log.Println("node ", rn.id, "- AppendEntries: leader is outdated")
		reply.Success = false
	}else if args.PrevLogIndex > 0 && int32(len(rn.log)) <= args.PrevLogIndex{ 
		// receiver node do not have matching PrevLog Index
		log.Println("node ", rn.id, "- AppendEntries: No matching PrevLog Index")
		reply.Success = false
	}else if args.PrevLogIndex > 0 && rn.log[args.PrevLogIndex].Term != args.PrevLogTerm{ 
		// receiver node PrevLog Term is not matching
		log.Println("node ", rn.id, "- AppendEntries: No matching PrevLog Term")
		reply.Success = false
	}

	// Handle if it is successful
	if reply.Success && args.Entries != nil{
		// 1. Delete the conflict log entries (if not same log, delete from follower)
		var i int32 = 1
		var newEntriesMatchCount int32 = 0
		for i = 1; args.PrevLogIndex + i < int32(len(rn.log)) && i <= int32(len(args.Entries)); i++{
			//log.Println("node ", rn.id, "- AppendEntries delete conflict logs, i:", i, "args.PrevLogIndex + i:", args.PrevLogIndex + i, "len(rn.log):", len(rn.log))

			// existing log conflicts with new log in sendLog
			if rn.log[args.PrevLogIndex + i].Term != args.Entries[i-1].Term{
				rn.log = rn.log[:args.PrevLogIndex + i]  // delete the conflict log
				break
				//TODO: bug if rn.log longer than sendLog
			}else{
				newEntriesMatchCount++
			}
		}
		//log.Println("node ", rn.id, "- AppendEntries deleted conflict logs, rn.log:", rn.log)

		// 2. Append new entries not in the log (append leader log to follower)
		if newEntriesMatchCount > 0 && int32(len(args.Entries)) > newEntriesMatchCount{
			rn.log = append(rn.log, args.Entries[newEntriesMatchCount:]...)
		}else if newEntriesMatchCount == 0{
			rn.log = append(rn.log, args.Entries...)
		}
		
		reply.MatchIndex = int32(len(rn.log) - 1)
		log.Println("node ", rn.id, "- AppendEntries appended logs, rn.log:", rn.log, "MatchIndex:", reply.MatchIndex)
	}else if reply.Success && args.Entries == nil{
		// Heartbeat
		reply.MatchIndex = int32(args.PrevLogIndex)
	}

	// Apply when committed
	if args.LeaderCommit > rn.commitIndex{
		log.Println("node ", rn.id, "- AppendEntries: Entered Commit Phase")
		// minIndex = min(follower lastLogIndex, leader CommitIndex)
		minIndex := int32(len(rn.log) - 1)
		if args.LeaderCommit < int32(len(rn.log) - 1){
			minIndex = args.LeaderCommit
		}
		for i := rn.commitIndex + 1; i <= minIndex; i++{
			// Apply the operation to kvMap of the follower
			if rn.log[i].Op == raft.Operation_Put{
				rn.kvMap[rn.log[i].Key] = rn.log[i].Value
			}else if rn.log[i].Op == raft.Operation_Delete{
				delete(rn.kvMap, rn.log[i].Key)
			}
		}
		rn.commitIndex = minIndex
		log.Println("node ", rn.id, "- AppendEntries committed, rn.commitIndex:", rn.commitIndex)
	}
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
	rn.electionTimeout = args.Timeout // update electionTimeout
	rn.resetChan <- true // reset electionTimeout
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
	rn.heartBeatInterval = args.Interval // update heartBeatInterval
	rn.resetChan <- true  // reset heartBeatTimeout
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
