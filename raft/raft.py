"""
Raft Consensus Algorithm Implementation

A simplified implementation of the Raft consensus algorithm
"""
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import time
import random
import hashlib


class NodeState(Enum):
    """Raft node states"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """Raft log entry"""
    term: int
    index: int
    command: str


@dataclass
class RequestVote:
    """RequestVote RPC"""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    """RequestVote RPC response"""
    term: int
    vote_granted: bool


@dataclass
class AppendEntries:
    """AppendEntries RPC"""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """AppendEntries RPC response"""
    term: int
    success: bool
    match_index: int = 0


class RaftNode:
    """
    Raft Consensus Node
    
    Simplified implementation for educational purposes
    """
    
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        
        # State
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Leadership
        self.leader_id: Optional[str] = None
        
        # Election
        self.election_timeout = random.uniform(150, 300)  # ms
        self.last_election_time = time.time()
        self.votes_received: Set[str] = set()
        
        # Replication
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader activity
        self.heartbeat_interval = 50  # ms
        
    def become_follower(self, term: int):
        """Become follower"""
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = None
    
    def become_candidate(self):
        """Become candidate and start election"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.last_election_time = time.time()
        
        # Request votes from all peers
        self.request_votes()
    
    def become_leader(self):
        """Become leader"""
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        
        # Initialize replication state
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0
        
        # Send initial heartbeat
        self.send_heartbeat()
    
    def request_votes(self):
        """Request votes from peers"""
        for peer in self.peers:
            # In real implementation, send RPC
            print(f"[{self.node_id}] Requesting vote from {peer}")
    
    def handle_request_vote(self, request: RequestVote) -> RequestVoteResponse:
        """Handle RequestVote RPC"""
        # Grant vote if:
        # 1. Term >= current_term
        # 2. Haven't voted for someone else OR voted for candidate
        # 3. Candidate's log is at least as up-to-date
        
        if request.term > self.current_term:
            self.become_follower(request.term)
        
        vote_granted = False
        
        if request.term >= self.current_term:
            if self.voted_for is None or self.voted_for == request.candidate_id:
                # Check log up-to-date
                last_log_idx = len(self.log) - 1
                last_log_term = self.log[last_log_idx].term if self.log else 0
                
                if (request.last_log_term > last_log_term or 
                    (request.last_log_term == last_log_term and 
                     request.last_log_index >= last_log_idx)):
                    vote_granted = True
                    self.voted_for = request.candidate_id
                    self.last_election_time = time.time()
        
        return RequestVoteResponse(
            term=self.current_term,
            vote_granted=vote_granted
        )
    
    def handle_append_entries(self, request: AppendEntries) -> AppendEntriesResponse:
        """Handle AppendEntries RPC"""
        # Leader validation
        if request.term > self.current_term:
            self.become_follower(request.term)
        
        # Not from current term leader
        if request.term < self.current_term:
            return AppendEntriesResponse(
                term=self.current_term,
                success=False
            )
        
        # Update election timeout
        self.last_election_time = time.time()
        self.leader_id = request.leader_id
        self.state = NodeState.FOLLOWER
        
        # Check log consistency
        if request.prev_log_index > 0:
            if request.prev_log_index > len(self.log):
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    match_index=0
                )
            
            # Check term match
            if request.prev_log_index <= len(self.log):
                entry = self.log[request.prev_log_index - 1]
                if entry.term != request.prev_log_term:
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        match_index=0
                    )
        
        # Append new entries
        if request.entries:
            # Find conflict and remove
            for i, entry in enumerate(request.entries):
                idx = request.prev_log_index + 1 + i
                if idx <= len(self.log):
                    if self.log[idx - 1].term != entry.term:
                        # Truncate
                        self.log = self.log[:idx - 1]
                        break
                else:
                    break
            
            # Append new entries
            start_idx = request.prev_log_index + 1
            new_entries = request.entries[start_idx - request.prev_log_index - 1:]
            self.log.extend(new_entries)
        
        # Update commit index
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))
        
        return AppendEntriesResponse(
            term=self.current_term,
            success=True,
            match_index=len(self.log)
        )
    
    def send_heartbeat(self):
        """Send heartbeat to all followers"""
        if self.state != NodeState.LEADER:
            return
        
        for peer in self.peers:
            # In real implementation, send RPC
            pass
    
    def replicate_log(self, entry: LogEntry) -> bool:
        """Replicate log entry to majority"""
        if self.state != NodeState.LEADER:
            return False
        
        self.log.append(entry)
        
        # Send to peers
        success_count = 1  # Leader
        
        for peer in self.peers:
            # In real implementation, send AppendEntries
            success_count += 1
        
        # Commit if majority
        if success_count > len(self.peers) // 2:
            self.commit_index = len(self.log)
            return True
        
        return False
    
    def tick(self):
        """Tick - called periodically"""
        now = time.time()
        
        # Election timeout
        if self.state != NodeState.LEADER:
            if now - self.last_election_time > self.election_timeout / 1000:
                self.become_candidate()
        
        # Heartbeat
        if self.state == NodeState.LEADER:
            if now - self.last_election_time > self.heartbeat_interval / 1000:
                self.send_heartbeat()
                self.last_election_time = now
    
    def get_status(self) -> Dict:
        """Get node status"""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "leader": self.leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index
        }


class RaftCluster:
    """
    Raft Cluster Management
    """
    
    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, RaftNode] = {}
        
        for node_id in node_ids:
            peers = [n for n in node_ids if n != node_id]
            self.nodes[node_id] = RaftNode(node_id, peers)
    
    def get_leader(self) -> Optional[RaftNode]:
        """Get current leader"""
        for node in self.nodes.values():
            if node.state == NodeState.LEADER:
                return node
        return None
    
    def get_status(self) -> Dict:
        """Get cluster status"""
        return {
            node_id: node.get_status()
            for node_id, node in self.nodes.items()
        }


# ============== Demo ==============

def demo():
    """Demo Raft consensus"""
    print("=" * 60)
    print("Raft Consensus Algorithm Demo")
    print("=" * 60)
    
    # Create cluster
    cluster = RaftCluster(["node1", "node2", "node3", "node4", "node5"])
    
    print("\nInitial Cluster Status:")
    for node_id, status in cluster.get_status().items():
        print(f"  {node_id}: {status['state']} (term={status['term']})")
    
    # Simulate election
    print("\nSimulating election...")
    
    # Make node1 become leader
    node1 = cluster.nodes["node1"]
    node1.become_candidate()
    node1.become_leader()
    
    print(f"\nAfter election: {cluster.get_leader().node_id} is leader")
    
    # Add log entry
    print("\nAdding log entry...")
    entry = LogEntry(term=1, index=1, command="set x = 10")
    success = node1.replicate_log(entry)
    print(f"Replicated: {success}")
    
    print("\n" + "=" * 60)
    print("Raft provides:")
    print("- Leader election")
    print("- Log replication")
    print("- Consensus")
    print("=" * 60)


if __name__ == "__main__":
    demo()
