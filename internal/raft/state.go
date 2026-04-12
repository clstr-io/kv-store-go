package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type raftState struct {
	CurrentTerm int     `json:"current-term"`
	VotedFor    *string `json:"voted-for"`
}

func (n *Node) statePath() string {
	return filepath.Join(n.dataDir, "raft-state.json")
}

func (n *Node) saveState() error {
	data, err := json.Marshal(raftState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	})
	if err != nil {
		return err
	}

	tmp := n.statePath() + ".tmp"
	err = os.WriteFile(tmp, data, 0644)
	if err != nil {
		return err
	}

	return os.Rename(tmp, n.statePath())
}

func (n *Node) loadState() error {
	data, err := os.ReadFile(n.statePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	var s raftState
	err = json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	n.currentTerm = s.CurrentTerm
	n.votedFor = s.VotedFor

	return nil
}
