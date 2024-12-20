use std::collections::{HashMap, HashSet};

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

fn node_num(value: &String) -> u32 {
    value
        .split_at(1)
        .1
        .parse::<u32>()
        .expect("Could not parse this node's node id")
}

fn node_group(value: &String) -> u32 {
    node_num(value) % N_GROUPS
}

struct BroadcastNode {
    id: String,
    msg_id: usize,
    output: RwLock<BufWriter<Stdout>>,
    /// Data this node has seen
    seen_data: HashSet<usize>,
    /// Status for each peer/datum
    peer_data: HashMap<String, HashMap<usize, DatumStatus>>,
}

impl MsgHandler<BroadcastMessages> for BroadcastNode {
    fn new(partial_node: glomers::PartialNode) -> Self
    where
        Self: MsgHandler<BroadcastMessages>,
    {
        // Topology - ring-tree style with branches
        // connected to at least one other branch for redundancy
        // Every node should have just two peers and is at worst 6 hops away
        // under double partition (for n=25). Just 4 hops normally.
        //
        // Almost all of my speed up from 3d -> 3e is due to a much more efficient topology!
        let this_node_num = node_num(&partial_node.id);
        let this_group = node_group(&partial_node.id);

        let mut peer_data = HashMap::new();

        for other_node in partial_node.node_ids {
            let other_node_num = other_node
                .split_at(1)
                .1
                .parse::<u32>()
                .expect("Could not parse node id");
            let other_group = node_group(&other_node);

            if this_node_num < N_GROUPS {
                // Trunk node - connected in ring and to all branches in group
                if other_group == this_group
                    || other_node_num == (5 + this_node_num + 1) % N_GROUPS
                    || other_node_num == (5 + this_node_num - 1) % N_GROUPS
                {
                    peer_data.insert(other_node.clone(), HashMap::new());
                }
            } else {
                // Branch node - connected to trunk node and one other branch
                if other_node_num == this_group
                    || (other_node_num == this_node_num + 5
                        && (other_node_num % (2 * N_GROUPS) == this_group))
                    || (other_node_num + 5 == this_node_num
                        && (this_node_num % (2 * N_GROUPS) == this_group))
                {
                    peer_data.insert(other_node.clone(), HashMap::new());
                }
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
            BroadcastMessages::Topology {
                topology: ref _topology,
            } => {
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
