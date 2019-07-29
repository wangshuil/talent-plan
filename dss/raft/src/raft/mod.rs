use std::sync::{Arc, Mutex};

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labcodec;
use labrpc::RpcFuture;

use rand::Rng;

use std::cmp;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::{thread, time};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

const TIMEOUT_RANGE_L: u64 = 200;
const TIMEOUT_RANGE_H: u64 = 350;
const HEARTBEAT_RANGE_L: u64 = 95;
const HEARTBEAT_RANGE_H: u64 = 100;

const MAX_ENTRIES_ONCE: u64 = 100;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => {
        //println!("{} :: {}", line!(), format_args!($($arg)*));
    };
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Message, Clone)]
pub struct PersistentState {
    #[prost(uint64, tag="1")]
    term: u64,
    #[prost(bool, tag="2")]
    is_leader: bool,
    #[prost(bytes, repeated, tag="3")]
    log: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LogEntry {
    // Your data here (2A).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bytes, tag = "2")]
    pub log_value: Vec<u8>,
}

impl LogEntry {
    fn new(term: u64, log_value: Vec<u8>) -> Self {
        LogEntry { term, log_value }
    }
}

#[derive(PartialEq)]
pub enum TimeoutSig {
    TIMEOUT,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    apply_ch: UnboundedSender<ApplyMsg>,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voted_for: Option<u64>,
    log: Vec<LogEntry>,
    //Volatile state on all servers:
    commit_index: u64,
    last_applied: u64,
    //Volatile state on leaders:
    next_index: Option<Vec<u64>>,
    match_index: Option<Vec<u64>>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            apply_ch,
            state: Arc::default(),
            voted_for: None,
            log: vec![LogEntry::new(0, vec![])],
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    pub fn get_peers(&self) -> Vec<RaftClient> {
        self.peers.clone()
    }

    pub fn get_peers_num(&self) -> usize {
        self.peers.len()
    }

    pub fn get_id(&self) -> usize {
        self.me
    }

    pub fn term(&self) -> u64 {
        self.state.term()
    }

    pub fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    pub fn set_state(&mut self, term: u64, is_leader: bool) {
        let state = State { term, is_leader };
        self.state = Arc::new(state);
        if self.is_leader() {
            self.next_index = Some(vec![self.log.len() as u64; self.peers.len()]);
            self.match_index = Some(vec![0; self.peers.len()]);
        } else {
            self.next_index = None;
            self.match_index = None;
        }
        self.persist();
    }

    pub fn get_voted_for(&self) -> Option<u64> {
        self.voted_for
    }

    pub fn set_voted_for(&mut self, id: Option<u64>) {
        self.voted_for = id;
    }

    pub fn get_last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    pub fn get_last_log_term(&self) -> u64 {
        self.log[self.get_last_log_index()].term
    }

    pub fn get_next_index(&self) -> Option<Vec<u64>> {
        self.next_index.clone()
    }

    pub fn get_commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        match self.log.get(index as usize) {
            Some(entry) => Some(entry.clone()),
            None => None,
        }
    }

    pub fn delete_entry(&mut self, index: u64) {
        let delete_set: Vec<LogEntry> = self.log.drain((index as usize)..).collect();
        self.persist();
        for item in delete_set {
            my_debug!("Node {} delete entry:{:?}", self.me, item);
        }
    }

    pub fn append_entry(&mut self, entry: LogEntry) {
        my_debug!(
            "Node {} append entry:[{}:{}] to local log!",
            self.get_id(),
            self.log.len(),
            entry.term
        );
        self.log.push(entry);
        self.persist();
    }

    ///this function is called when append_entries get a success reply
    ///leader will update its next_index and match_index vector to record follower's log state
    ///besides, leader check weather it can commit this entry
    /// only those logs with a term == leader.term and be stored by half of nodes can be committed
    pub fn update_and_commit(&mut self, id: usize, index: u64) {
        if self.next_index == None || self.match_index == None {
            return;
        }
        let mut match_index = self.match_index.clone().unwrap();
        let mut next_index = self.next_index.clone().unwrap();
        my_debug!(
            "Node {} --- before update --- next_index:{:?} match_index:{:?}",
            self.me,
            next_index,
            match_index
        );
        next_index[id] = index;
        match_index[id] = next_index[id] - 1;
        self.match_index = Some(match_index.clone());
        self.next_index = Some(next_index.clone());
        my_debug!(
            "Node {} --- after update --- next_index:{:?} match_index:{:?}",
            self.me,
            next_index,
            match_index
        );
        let mut update_commit_index: u64 = 0;
        let new_match_index = self.match_index.clone().unwrap();
        for scan_index in ((self.commit_index + 1)..(self.log.len() as u64)).rev() {
            let mut agree_node: u64 = 0;
            let num = self.get_peers_num() as usize;
            for i in 0..num {
                if i == self.me {
                    continue;
                }
                if match_index[i] >= scan_index {
                    agree_node += 1;
                }
            }
            if (agree_node + 1) > ((num as u64) / 2) {
                update_commit_index = scan_index;
                break;
            }
        }
        if update_commit_index != 0 {
            if self.get_entry(update_commit_index).unwrap().term != self.term() {
                return;
            }
            self.set_commit_index(update_commit_index);
        }
    }

    ///this function is called when we updated a new commit index and need to send msg to client
    /// we send out a ApplyMsg to confirm this commit
    pub fn set_commit_index(&mut self, new_commit_index: u64) {
        my_debug!(
            "Node {} set commit index:[{}->{}]",
            self.me,
            self.commit_index,
            new_commit_index
        );
        self.commit_index = new_commit_index;
        if self.commit_index > self.last_applied {
            for i in self.last_applied..self.commit_index {
                self.last_applied += 1;
                let msg = ApplyMsg {
                    command_valid: true,
                    command: self.log[self.last_applied as usize].log_value.clone(),
                    command_index: self.last_applied,
                };
                let _ = self.apply_ch.unbounded_send(msg);
                my_debug!(
                    "Node {} apply entry {} to state machine",
                    self.me,
                    self.last_applied
                );
            }
        }
    }

    pub fn decrease_next_index(&mut self, id: usize) {
        if self.next_index == None {
            return;
        }
        let mut next_index = self.next_index.clone().unwrap();
        my_debug!(
            "Node {} --- before decrease --- next_index:{:?}",
            self.me,
            next_index
        );
        if next_index[id] < (MAX_ENTRIES_ONCE + 1) {
            next_index[id] = 1;
        } else {
            next_index[id] -= MAX_ENTRIES_ONCE;
        }
        self.next_index = Some(next_index.clone());
        my_debug!(
            "Node {} --- after decrease --- next_index:{:?}",
            self.me,
            next_index
        );
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = vec![];
        let mut state = PersistentState {
            term: self.term(),
            is_leader: self.is_leader(),
            log: vec![],
        };
        for i in 1..self.log.len() {
            let mut buf = vec![];
            let entry = self.log[i].clone();
            let _ = labcodec::encode(&entry, &mut buf).map_err(Error::Encode);
            state.log.push(buf);
        }
        let _ = labcodec::encode(&state, &mut data).map_err(Error::Encode);
        self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode(data) {
            Ok(state) => {
                let state: PersistentState = state;
                let raftstate = State {
                    term: state.term,
                    is_leader: state.is_leader,
                };

                self.state = Arc::new(raftstate);
                for i in 0..state.log.len() {
                    let entry = state.log[i].clone();
                    match labcodec::decode(&entry) {
                        Ok(log_entry) => {
							let en: LogEntry = log_entry;
                            self.log.push(en.clone());
                            my_debug!("Node {} restore log: {:?}", self.me, en);
                        },
                        Err(e) => {
                            panic!("failed to decode entry: {:?}", e);
                        },
                    }
                }
                my_debug!("Node {} restore state: {:?}", self.me, state);
            },
            Err(e) => {
                panic!("recover error: {:?}", e);
            },
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        my_debug!("get start with cmd ");
        let index = self.log.len() as u64;
        let term = self.term();
        //let wait_time=time::Duration::from_millis(TIMEOUT_RANGE_H+10);
        //thread::sleep(wait_time);
        let is_leader = self.is_leader();
        if is_leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // Your code here (2B).
            let entry = LogEntry::new(term, buf);
            self.append_entry(entry);

            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
    timeout_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    vote_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    append_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    timeout_flag: Arc<AtomicBool>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            timeout_thread: Arc::new(Mutex::new(None)),
            vote_thread: Arc::new(Mutex::new(None)),
            append_thread: Arc::new(Mutex::new(None)),
            timeout_flag: Arc::new(AtomicBool::new(false)),
        };
        node.start_threads();
        node
    }

    pub fn start_threads(&self) {
        let id = self.get_id();
        my_debug!("Boost up {} Node", id);
        let (tx, rx) = mpsc::channel();
        // create 3 threads to check timeout,do vote and do append
        let node1 = self.clone();
        let id1 = id; //1 copy for move
        let timeout_thread_impl = thread::spawn(move || {
            node1.run_timeout_thread(tx.clone());
        });
        *self.timeout_thread.lock().unwrap() = Some(timeout_thread_impl);
        let node2 = self.clone();
        let id2 = id; //1 copy for move
        let vote_thread_impl = thread::spawn(move || {
            node2.run_vote_thread(rx);
        });
        *self.vote_thread.lock().unwrap() = Some(vote_thread_impl);
        let node3 = self.clone();
        let id3 = id; //1 copy for move
        let append_thread_impl = thread::spawn(move || {
            node3.run_append_thread();
        });
        *self.append_thread.lock().unwrap() = Some(append_thread_impl);
    }

    /// functions to run or stop threads
    ///
    /// timeout thread is a thread for followers to check the heartbeat from leader
    /// leader don't need this thread so it's parked
    /// if follower or candidater get timeout, it will send sig to vote thread to be a candidater
    /// random fallback alg is implemented here by choosing a random timeout limit
    pub fn run_timeout_thread(&self, sender: Sender<TimeoutSig>) {
        let node = self.clone();
        loop {
            if node.is_leader() {
                my_debug!("Node {} is leader, timeout thread stop!", node.get_id());
                thread::park();
            }
            node.timeout_flag.store(false, Ordering::Relaxed);
            let random_fallback_time = time::Duration::from_millis(
                rand::thread_rng().gen_range(TIMEOUT_RANGE_L, TIMEOUT_RANGE_H),
            );
            thread::park_timeout(random_fallback_time);
            if false == node.timeout_flag.load(Ordering::Relaxed) {
                my_debug!(
                    "Node {} --- follower timeout! send signal to the vote thread!",
                    node.get_id()
                );
                let newsender = sender.send(TimeoutSig::TIMEOUT);
            }
        }
    }

    //this function is actually called by other thread to unleash this thread
    //when a node become follower,this func ill be called
    pub fn unpark_timeout_thread(&self) {
        match *self.timeout_thread.lock().unwrap() {
            Some(ref tthread) => {
                my_debug!(
                    "Node {} is no longer leader, wakeup timeout thread!",
                    self.get_id()
                );
                tthread.thread().unpark();
            }
            None => {
                return;
            }
        }
    }

    pub fn set_timeout_flag(&self) {
        self.timeout_flag.store(true, Ordering::Relaxed);
        my_debug!("node {} set_timeout_flag",self.get_id());
    }

    /// vote thread is for candidater to requst followers to vote
    /// no action would be taken when timeout don't happen
    /// as a candidater,node inc its term,send request to all servers
    /// when it gets more than peers_num/2,it will become a leader and start working
    /// when there is another leader,it will become a follower
    pub fn run_vote_thread(&self, receiver: Receiver<TimeoutSig>) {
        let node = self.clone();
        loop {
            if TimeoutSig::TIMEOUT == receiver.recv().unwrap() {
                let mut term = node.term();
                term += 1;
                node.set_state(term, false);
                my_debug!(
                    "Node {} --- vote thread receive signal, convert to candidate, term {}!",
                    node.get_id(),
                    node.term()
                );
                let id = node.get_id();
                node.set_voted_for(Some(id as u64));
                let sendargs = RequestVoteArgs {
                    term,
                    candidate_id: id as u64,
                    last_log_index: node.get_last_log_index() as u64,
                    last_log_term: node.get_last_log_term(),
                };
                let peers = node.get_peers();
                let num = node.get_peers_num();
                let vote_count = Arc::new(Mutex::new(1));
                for i in 0..num {
                    if i == id as usize {
                        continue;
                    }
                    //take ownership for move
                    let temp_node = node.clone();
                    let temp_args = sendargs.clone();
                    let temp_vote_count = Arc::clone(&vote_count);
                    let peer = peers[i].clone();
                    let temp_term = term;
                    let temp_id = id;
                    my_debug!("candidate {} request vote from {} in term {}", temp_id, i,temp_term);
                    thread::spawn(move || {
                        let rpl = peer.request_vote(&temp_args).map_err(Error::Rpc).wait();
                        match rpl {
                            Ok(reply) => {
                                if reply.vote_granted {
                                    *temp_vote_count.lock().unwrap() += 1;
                                    if *temp_vote_count.lock().unwrap() > num / 2
                                        && !temp_node.is_leader()
                                        && temp_node.term() == temp_term
                                    {
                                        temp_node.set_state(temp_term, true);
                                        temp_node.send_append_entries();
                                        temp_node.unpark_append_thread();
                                        temp_node.set_timeout_flag();
                                    }
                                } else if reply.term > temp_node.term() {
                                    my_debug!(
                                        "candidate {} find leader, convert to follower!",
                                        temp_id
                                    );
                                    temp_node.set_state(reply.term, false);
                                    temp_node.set_voted_for(None);
                                    temp_node.set_timeout_flag();
                                }
                            }
                            Err(_) => {
                                my_debug!("candidate {} failed to request vote from {} in term {}", temp_id, i,temp_term);
                            }
                        }
                    });
                }
            }
        }
    }

    ///append thread is for leader to send log and heartbeat
    ///it works like a timeout thread ,only changes recv to send and owned by leader
    pub fn run_append_thread(&self) {
        let node = self.clone();
        loop {
            if !node.is_leader() {
                thread::park();
            }
            // sleep a random time around 100ms
            let sleep_time =rand::thread_rng().gen_range(HEARTBEAT_RANGE_L, HEARTBEAT_RANGE_H);
            let sleep_dur = time::Duration::from_millis(sleep_time);
            my_debug!("leader {} wait {} time to send heartbeat",self.get_id(),sleep_time);
            thread::park_timeout(sleep_dur);
            // send heartbeat to followers (10 times per second)
            if node.is_leader() {
                node.send_append_entries();
            }
        }
    }

    pub fn unpark_append_thread(&self) {
        match *self.append_thread.lock().unwrap() {
            Some(ref athread) => {
                //println!("wakeup {} node's append thread",self.get_id());
                athread.thread().unpark();
            }
            None => {
                return;
            }
        }
    }

    ///send_append_entries send heartbeats and logentries to followers
    /// it's running logic is like vote thread sending msg for vote
    /// we pack up some logentries to send them together
    ///
    pub fn send_append_entries(&self) {
        let node = self.clone();
        if !node.is_leader() {
            return;
        }
        let term = node.term();
        let id = node.get_id();
        my_debug!("leader {} send heart beat, term {}", id, term);
        let next_index = match node.get_next_index() {
            Some(nindex) => nindex,
            None => {
                return;
            }
        };
        let peers = node.get_peers();
        let num = node.get_peers_num();
        for i in 0..num {
            if i == id as usize {
                continue;
            }
            let prev_log_index = next_index[i] - 1;
            let temp_node = node.clone();
            let prev_log_term = match temp_node.get_entry(prev_log_index) {
                Some(entry) => entry.term,
                None => 0,
            };
            //pack entries
            let mut packed_entries = Vec::new();
            for j in 0..MAX_ENTRIES_ONCE {
                let temp_entry = temp_node.get_entry(next_index[i] + j);
                match temp_entry {
                    Some(entry) => {
                        let mut buffer = vec![];
                        let _ = labcodec::encode(&entry, &mut buffer).map_err(Error::Encode);
                        packed_entries.push(buffer);
                    }
                    None => {
                        break;
                    }
                }
            }
            let sendargs = AppendEntriesArgs {
                term,
                leader_id: id as u64,
                prev_log_index,
                prev_log_term,
                leader_commit: temp_node.get_commit_index(),
                entries: packed_entries,
            };
            let peer = peers[i].clone();
            let temp_term = term;
            let temp_id = id;
            my_debug!("leader {} send heart beat to follower {}", id, i);
            thread::spawn(move || {
                let reply = peer.append_entries(&sendargs).map_err(Error::Rpc).wait();
                match reply {
                    Ok(rplmsg) => {
                        if rplmsg.success {
                            my_debug!(
                                "leader {} receive append entries reply(success) from {}",
                                temp_id,
                                i
                            );
                            if rplmsg.term == temp_node.term() {
                                temp_node.update_and_commit(i, rplmsg.next_index);
                            }
                        } else {
                            my_debug!("leader {} receive heart beat reply(false) from {}", temp_id, i);
                            if rplmsg.term > temp_node.term() {
                                my_debug!("leader receive term {} bigger than current term {}, convert to follower!", rplmsg.term, temp_node.term());
                                temp_node.set_state(rplmsg.term, false);
                                temp_node.set_voted_for(None);
                                temp_node.unpark_timeout_thread();
                            } else {
                                my_debug!("decrease next index and retry");
                                if rplmsg.term == temp_node.term() {
                                    temp_node.decrease_next_index(i);
                                }
                            }
                        }
                    }
                    Err(_) => {
                        my_debug!(
                            "leader {} failed to send heart beat, term {}!",
                            temp_id,
                            temp_term
                        );
                    }
                }
            });
        }
    }
    /*



    */
    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
         if self.is_leader() {
            let res = self.raft.lock().unwrap().start(command);
            res
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        //self.raft.state.term
        self.raft.lock().unwrap().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// functions to get/set peer's state
    pub fn set_state(&self, term: u64, is_leader: bool) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_state(term, is_leader);
    }

    pub fn get_id(&self) -> usize {
        self.raft.lock().unwrap().get_id()
    }

    pub fn get_voted_for(&self) -> Option<u64> {
        self.raft.lock().unwrap().get_voted_for()
    }

    pub fn set_voted_for(&self, id: Option<u64>) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_voted_for(id);
    }

    pub fn get_peers_num(&self) -> usize {
        self.raft.lock().unwrap().get_peers_num()
    }

    pub fn get_peers(&self) -> Vec<RaftClient> {
        self.raft.lock().unwrap().get_peers()
    }

    pub fn get_last_log_index(&self) -> usize {
        self.raft.lock().unwrap().get_last_log_index()
    }

    pub fn get_last_log_term(&self) -> u64 {
        self.raft.lock().unwrap().get_last_log_term()
    }

    pub fn get_next_index(&self) -> Option<Vec<u64>> {
        self.raft.lock().unwrap().get_next_index()
    }

    pub fn get_commit_index(&self) -> u64 {
        self.raft.lock().unwrap().get_commit_index()
    }

    pub fn set_commit_index(&self, index: u64) {
        let mut raft = self.raft.lock().unwrap();
        raft.set_commit_index(index);
    }

    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        self.raft.lock().unwrap().get_entry(index)
    }

    pub fn delete_entry(&self, index: u64) {
        let mut raft = self.raft.lock().unwrap();
        raft.delete_entry(index);
    }

    /// append entry to log
    pub fn append_entry(&self, entry: LogEntry) {
        let mut raft = self.raft.lock().unwrap();
        raft.append_entry(entry);
    }

    pub fn update_and_commit(&self, id: usize, index: u64) {
        let mut raft = self.raft.lock().unwrap();
        raft.update_and_commit(id, index);
    }

    pub fn decrease_next_index(&self,id: usize) {
        let mut raft = self.raft.lock().unwrap();
        raft.decrease_next_index(id);
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.

    ///if request has a lower termm or older log, refuse to vote for it
    /// set vote once
    /// start timeout timer
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut do_vote=true;
        let now_term=self.term();
        if args.term<self.term(){
            do_vote=false;
        }
        else {
            if args.last_log_term<self.get_last_log_term() {
                do_vote=false;
            }
            else if args.last_log_term==self.get_last_log_term() && args.last_log_index<self.get_last_log_index() as u64{
                do_vote=false;
            }
            else{
                my_debug!("Node {} vote for Node {} in term {}", self.get_id(), args.candidate_id,args.term);
		    	self.set_voted_for(Some(args.candidate_id));
            }

            if self.is_leader() {
                self.unpark_timeout_thread();
            } else {
                self.set_timeout_flag();
            }

            self.set_state(args.term, false);
        }
        let reply=RequestVoteReply{
            term:now_term,
            vote_granted:do_vote
        };
        Box::new(futures::future::result(Ok(reply)))
    }
    
    ///this function is called to append entries in node
    /// there are 3 different reason for a failure
    /// 1st,receive a msg with a smaller term
    ///     we just throw it away and send Reply with our new bigger term
    ///     the sender will recv it and know that it is no more a leader 
    /// 2nd,receive a msg,but prev_entry is different from sender 
    ///     we should roll back this node's log to find a match
    ///     decrease_next_index will be called to find a match
    /// 3rd,receive a msg,but prev_entry is empty
    ///     like the situation in 2nd,roll back and call decrease_next_index
    /// 
    /// when success,modify loacl entries and commit if necessary
    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        let mut now_term=self.term();
        let mut success=true;
        let mut next_index=0;
        if args.term<now_term{
            success=false;
        }
        else{
            let prev_entry=self.get_entry(args.prev_log_index);
            match prev_entry {
                Some(entry)=>{
                    if entry.term!=args.prev_log_term {
                        success=false;
                    }
                    else{
                        if args.entries.len()==0{
                            my_debug!("Node {} receive heart beat!", self.get_id());
                            //nothing will be done
                        }
                        else{
                            my_debug!("Node {} receive append entries!", self.get_id());
                            for i in 0..args.entries.len(){
                                match labcodec::decode(&args.entries[i]) {
									Ok(args_entry) => {
										let entry = self.get_entry(args.prev_log_index + (i as u64) + 1);
										match entry {
											Some(local_entry) => {
												if local_entry == args_entry {
													continue;
												} else {
													self.delete_entry(args.prev_log_index + (i as u64) + 1);
													self.append_entry(args_entry);
												}
											},
											None => {
												self.append_entry(args_entry);
											}
										}
									},
									Err(e) => {
										panic!("Failed to decode append entry: {:?}", e);
                                    }
                                }
                            }
                        }
                        next_index = args.prev_log_index + 1 + args.entries.len() as u64;
                        let to_commit = cmp::min(args.leader_commit, next_index - 1);
                        if to_commit > self.get_commit_index() {
                            let new_commit_index = cmp::min(to_commit, self.get_last_log_index() as u64);
                            self.set_commit_index(new_commit_index);
                        }
                    }
                },
                None=>{
                    success=false;
                },
            }
            self.set_state(args.term, false);
            now_term = self.term();
        	self.set_voted_for(None);
        	self.set_timeout_flag();
        }
        let reply = AppendEntriesReply {
            term: now_term,
            success,
            next_index,
        };
        Box::new(futures::future::result(Ok(reply)))
    }
}
