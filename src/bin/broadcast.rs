use std::
    collections::{HashMap, HashSet}
;

use tokio::{
    io::{BufWriter, Stdout},
    sync::RwLock,
};

use glomers::{Body, Message, MsgHandler};
use serde::{Deserialize, Serialize};

const N_GROUPS: u32 = 5;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastMessages {
    Broadcast {
        message: usize,
    },
    Gossip {
        /// Data the client node should learn from this server
        data_you_need: HashSet<usize>,
        /// Data the server has recently learned from the client node
        data_i_received_from_you: HashSet<usize>,
    },
    Read,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    ReadOk {
        messages: HashSet<usize>,
    },
    BroadcastOk,
    TopologyOk,
}

enum DatumStatus {
    /// We know the datum and have sent it at least once, but don't know if they know it yet
    SentUnconfirmed,
    /// We both know the datum but they don't yet know that we now know it
    ReceivedUnconfirmed,
    /// We know they know the datum and they know we know the datum
    Confirmed,
}

struct BroadcastNode {
    id: String,
    msg_id: usize,
    output: RwLock<BufWriter<Stdout>>,
    /// Data this node has seen
    seen_data: HashSet<usize>,
    /// Status for each peer/datum
    peer_data: HashMap<String, HashMap<usize, DatumStatus>>,
    // /// Messages the peer thinks we know based on their behaviour
    // peer_view_of_me: HashMap<String, HashSet<usize>>,
}

impl MsgHandler<BroadcastMessages> for BroadcastNode {
    fn new(partial_node: glomers::PartialNode) -> Self
    where
        Self: MsgHandler<BroadcastMessages>,
    {
        // Topology - redundant fat-tree-like topology
        // Nodes in 5 groups, two nodes from each group are
        // cross-connected
        let this_node_id = partial_node.id.split_at(1).1.parse::<u32>().expect("Could not parse this node's node id");
        let mut peer_data = HashMap::new();
        for peer in partial_node.peers {
            let node_id = peer.split_at(1).1.parse::<u32>().expect("Could not parse node id");
            // Add all nodes in the same group i.e. 0, 5, 10, 15, 20
            if node_id % N_GROUPS == this_node_id % N_GROUPS {
                peer_data.insert(peer.clone(), HashMap::new());
            }
            // Cross-connect the first node in each group i.e. 0, 1, 2, 3, 4
            if this_node_id < N_GROUPS && node_id != this_node_id {
                peer_data.insert(peer.clone(), HashMap::new());
            }
            // Cross-connect the second node in each group i.e. 5, 6, 7, 8, 9
            if this_node_id < N_GROUPS + N_GROUPS && this_node_id >= N_GROUPS && node_id != this_node_id {
                peer_data.insert(peer.clone(), HashMap::new());
            }
        }
        BroadcastNode {
            id: partial_node.id,
            msg_id: partial_node.msg_id,
            output: partial_node.output,
            seen_data: HashSet::new(),
            peer_data,
        }
    }

    async fn handle_msg(&mut self, msg: Message<BroadcastMessages>)
    where
        BroadcastMessages: Serialize,
    {
        match msg.body.msg {
            BroadcastMessages::Broadcast { message } => {
                self.seen_data.insert(message);
                self.reply(&msg, BroadcastMessages::BroadcastOk).await;
            }
            BroadcastMessages::Read => {
                self.reply(
                    &msg,
                    BroadcastMessages::ReadOk {
                        messages: self.seen_data.clone(),
                    },
                )
                .await;
            }
            BroadcastMessages::Topology { ref topology } => {
                // // Remove peers that are no longer contained in the topology
                // let mut stale_peers = vec![];
                // for existing_peer in self.peer_data.keys() {
                //     if !topology.contains_key(existing_peer) {
                //         stale_peers.push(existing_peer.clone())
                //     }
                // }
                // for stale_peer in stale_peers {
                //     self.peer_data.remove(&stale_peer);
                // }

                // // Add any new peers in the topology
                // let peers = topology.get(&self.id).expect("topology should contain this node's node ID!");
                // for peer in peers {
                //     self.peer_data.entry(peer.clone()).or_insert(HashMap::new());
                // }

                // The commented code above is for using the provided topologies.
                // We ignore those topologies here and setup our own in `new`
                self.reply(&msg, BroadcastMessages::TopologyOk).await;
            }
            BroadcastMessages::Gossip {
                data_you_need,
                data_i_received_from_you,
            } => {
                // We don't reply to a gossip, we gossip on a schedule. This code handles *receiving* a gossip message
                let peer_data = self.peer_data.entry(msg.src).or_insert(HashMap::new());
                let mut new_data = Vec::new();
                for datum in data_you_need.difference(&self.seen_data) {
                    // If we haven't seen this entry before, register that we've seen it
                    let status = peer_data
                        .entry(*datum)
                        .or_insert(DatumStatus::ReceivedUnconfirmed);
                    *status = DatumStatus::ReceivedUnconfirmed;
                    new_data.push(*datum);
                }
                self.seen_data.extend(new_data);

                // All 'data I received from you' values are now confirmed.
                for datum in data_i_received_from_you {
                    *peer_data
                        .get_mut(&datum)
                        .expect("We should already be tracking a datum for this node") =
                        DatumStatus::Confirmed;
                }
            }
            BroadcastMessages::BroadcastOk
            | BroadcastMessages::ReadOk { messages: _ }
            | BroadcastMessages::TopologyOk => (),
        }
    }

    fn bg_task_interval_ms(&self) -> u64 {
        // Success on task 3d with 250ms gossip interval
        250
    }

    async fn bg_task(&mut self) {
        for data_state in self.peer_data.values_mut() {
            for datum in self.seen_data.iter() {
                if !data_state.contains_key(&datum) {
                    data_state.insert(*datum, DatumStatus::SentUnconfirmed);
                }
            }
        }

        for (count, (peer, data_state)) in self.peer_data.iter().enumerate() {
            let data_you_need = data_state
                .iter()
                .filter(|(_d, s)| matches!(**s, DatumStatus::SentUnconfirmed))
                .map(|(d, _s)| *d)
                .collect::<HashSet<_>>();
            let data_i_received_from_you = data_state
                .iter()
                .filter(|(_d, s)| matches!(s, DatumStatus::ReceivedUnconfirmed))
                .map(|(d, _s)| *d)
                .collect::<HashSet<_>>();

            let msg = Message {
                src: self.id.clone(),
                dst: peer.clone(),
                body: Body {
                    msg_id: Some(self.msg_id.clone() + count),
                    in_reply_to: None,
                    msg: BroadcastMessages::Gossip {
                        data_you_need,
                        data_i_received_from_you,
                    },
                },
            };
            self.send_msg_inner(self.get_output(), msg).await;
        }
        self.msg_id += self.peer_data.len();
    }

    fn get_msg_id(&mut self) -> &mut usize {
        &mut self.msg_id
    }

    fn get_output(&self) -> &RwLock<BufWriter<Stdout>> {
        &self.output
    }
}

#[tokio::main]
async fn main() {
    let jh = tokio::spawn(BroadcastNode::run::<BroadcastMessages>());
    jh.await.unwrap();
    panic!("Finished!!");
}

// echo '{"src":"c0","dest":"n3","body":{"type":"init","msg_id":1,"node_id":"n3","node_ids":["n1", "n2", "n3"]}}' | cargo run --bin broadcast
