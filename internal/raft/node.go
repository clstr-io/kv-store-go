package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/clstr-io/key-value-go/internal/store"
)

const (
	electionTimeoutMin = 500 * time.Millisecond
	electionTimeoutMax = 1_000 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond

	rpcTimeout = 50 * time.Millisecond
)

type Role int

const (
	RoleFollower = iota
	RoleCandidate
	RoleLeader
)

var roleNames = map[Role]string{
	RoleFollower:  "follower",
	RoleCandidate: "candidate",
	RoleLeader:    "leader",
}

type VoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate-id"`
	LastLogIndex int    `json:"last-log-index"`
	LastLogTerm  int    `json:"last-log-term"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote-granted"`
}

type AppendEntriesRequest struct {
	Term         int      `json:"term"`
	LeaderID     string   `json:"leader-id"`
	PrevLogIndex int      `json:"prev-log-index"`
	PrevLogTerm  int      `json:"prev-log-term"`
	Entries      []string `json:"entries"`
	LeaderCommit int      `json:"leader-commit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type ClusterInfoResponse struct {
	ID     string   `json:"id"`
	Role   string   `json:"role"`
	Term   int      `json:"term"`
	Leader *string  `json:"leader"`
	Peers  []string `json:"peers"`
}

type Node struct {
	id      string
	role    Role
	peers   []string
	dataDir string

	currentTerm int
	votedFor    *string
	leaderID    *string

	store         *store.DiskStore
	resetElection chan struct{}
	client        *http.Client

	lastQuorumAck time.Time

	mu sync.RWMutex
}

func NewNode(id string, peers []string, dataDir string) (*Node, error) {
	sto, err := store.NewDiskStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("cannot create disk store: %w", err)
	}

	n := Node{
		id:            id,
		peers:         peers,
		dataDir:       dataDir,
		store:         sto,
		resetElection: make(chan struct{}, 1),
		client:        &http.Client{Timeout: rpcTimeout},
	}

	err = n.loadState()
	if err != nil {
		return nil, fmt.Errorf("cannot load raft state: %w", err)
	}

	if len(peers) == 0 {
		n.role = RoleLeader
		n.leaderID = &n.id
		log.Print("Single-node mode, starting as leader")
	} else {
		n.role = RoleFollower
	}

	return &n, nil
}

func (n *Node) becomeFollower(term int, leaderID *string) {
	if n.currentTerm > term {
		return
	}

	if n.currentTerm == term && n.role == RoleFollower && ptrStr(n.leaderID) == ptrStr(leaderID) {
		return
	}

	if leaderID != nil {
		log.Printf("Following %s", *leaderID)
	}

	n.role = RoleFollower
	n.currentTerm = term
	n.votedFor = nil
	n.leaderID = leaderID

	err := n.saveState()
	if err != nil {
		log.Printf("Failed to save raft state: %v", err)
	}
}

func (n *Node) becomeCandidate() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = &n.id
	n.leaderID = nil

	err := n.saveState()
	if err != nil {
		log.Printf("Failed to save raft state: %v", err)
	}

	log.Printf("Starting election for term %d", n.currentTerm)
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = &n.id
	n.lastQuorumAck = time.Now()

	log.Printf("Became leader for term %d", n.currentTerm)
}

func (n *Node) electionTimeout() time.Duration {
	spread := (electionTimeoutMax - electionTimeoutMin).Milliseconds()
	return electionTimeoutMin + time.Duration(rand.Int64N(spread))*time.Millisecond
}

func (n *Node) Loop(ctx context.Context) {
	if len(n.peers) == 0 {
		<-ctx.Done()
		return
	}

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	election := time.NewTimer(n.electionTimeout())
	defer election.Stop()

	for {
		select {
		case <-election.C:
			n.mu.RLock()
			isLeader := n.role == RoleLeader
			n.mu.RUnlock()

			if !isLeader {
				n.startElection()
			}

			election.Reset(n.electionTimeout())
		case <-n.resetElection:
			if !election.Stop() {
				<-election.C
			}

			election.Reset(n.electionTimeout())
		case <-heartbeat.C:
			n.mu.RLock()
			isLeader := n.role == RoleLeader
			n.mu.RUnlock()

			if isLeader {
				n.sendHeartbeats()

				n.mu.Lock()
				if n.role == RoleLeader && time.Since(n.lastQuorumAck) > electionTimeoutMin {
					log.Printf("Lost quorum contact for term %d, stepping down", n.currentTerm)
					n.becomeFollower(n.currentTerm, nil)
				}
				n.mu.Unlock()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (n *Node) callRPC(peer, path string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	res, err := n.client.Post(peer+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return json.NewDecoder(res.Body).Decode(resp)
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.becomeCandidate()

	req := VoteRequest{
		Term:        n.currentTerm,
		CandidateID: n.id,
	}
	quorum := (len(n.peers)+1)/2 + 1
	n.mu.Unlock()

	type voteResult struct {
		peer string
		resp VoteResponse
	}

	var wg sync.WaitGroup
	results := make(chan voteResult, len(n.peers))
	for _, peer := range n.peers {
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()

			var resp VoteResponse
			err := n.callRPC(peer, "/raft/request-vote", req, &resp)
			if err != nil {
				return
			}
			results <- voteResult{peer: peer, resp: resp}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	votes := 1
	for r := range results {
		resp := r.resp
		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term, nil)
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		if resp.VoteGranted {
			votes++
			log.Printf("Received vote from %s (%d/%d) for term %d", r.peer, votes, quorum, req.Term)
		}

		if votes >= quorum {
			break
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if votes >= quorum && n.role == RoleCandidate && n.currentTerm == req.Term {
		n.becomeLeader()
	} else {
		n.role = RoleFollower
		log.Printf("Election for term %d failed", req.Term)
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	req := AppendEntriesRequest{
		Term:     n.currentTerm,
		LeaderID: n.id,
	}
	quorum := (len(n.peers)+1)/2 + 1
	n.mu.RUnlock()

	var (
		mu   sync.Mutex
		acks = 1 // count self
		wg   sync.WaitGroup
	)
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			var resp AppendEntriesResponse
			err := n.callRPC(peer, "/raft/append-entries", req, &resp)
			if err != nil {
				return
			}

			n.mu.Lock()
			if resp.Term > req.Term {
				n.becomeFollower(resp.Term, nil)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if resp.Success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == RoleLeader && acks >= quorum {
		n.lastQuorumAck = time.Now()
	}
}

func (n *Node) Vote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("Vote request from %s for term %d", req.CandidateID, req.Term)

	if req.Term < n.currentTerm {
		return VoteResponse{Term: n.currentTerm, VoteGranted: false}
	} else if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, nil)
	}

	if n.votedFor != nil && *n.votedFor != req.CandidateID {
		return VoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	n.votedFor = &req.CandidateID

	err := n.saveState()
	if err != nil {
		log.Printf("Failed to save raft state: %v", err)
	}

	log.Printf("Voted for %s in term %d", req.CandidateID, n.currentTerm)

	select {
	case n.resetElection <- struct{}{}:
	default:
	}

	return VoteResponse{Term: n.currentTerm, VoteGranted: true}
}

func (n *Node) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.currentTerm > req.Term {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, &req.LeaderID)
	} else {
		n.role = RoleFollower
		n.leaderID = &req.LeaderID
	}

	select {
	case n.resetElection <- struct{}{}:
	default:
	}

	return AppendEntriesResponse{Term: n.currentTerm, Success: true}
}

// LeaderID returns the current leader's address and whether this node is the leader.
func (n *Node) LeaderID() (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.leaderID == nil {
		return "", false
	}

	return *n.leaderID, n.role == RoleLeader
}

func (n *Node) Info() ClusterInfoResponse {
	n.mu.RLock()
	defer n.mu.RUnlock()

	peers := make([]string, len(n.peers))
	for i, p := range n.peers {
		peers[i] = strings.TrimPrefix(p, "http://")
	}

	return ClusterInfoResponse{
		ID:     n.id,
		Role:   roleNames[n.role],
		Term:   n.currentTerm,
		Leader: n.leaderID,
		Peers:  peers,
	}
}

func (n *Node) Set(key, value string) error {
	return n.store.Set(key, value)
}

func (n *Node) Get(key string) (string, error) {
	return n.store.Get(key)
}

func (n *Node) Delete(key string) error {
	return n.store.Delete(key)
}

func (n *Node) Clear() error {
	return n.store.Clear()
}

func (n *Node) Shutdown() error {
	err := n.store.Close()
	if err != nil {
		return fmt.Errorf("cannot close kv store: %w", err)
	}

	return err
}

func ptrStr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
