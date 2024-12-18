use std::time::{SystemTime, UNIX_EPOCH};

use tokio::{
    io::{BufWriter, Stdout},
    sync::RwLock,
};

use glomers::{Message, MsgHandler};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GenerateMessages {
    Generate,
    GenerateOk { id: String },
}

struct UniqueIdNode {
    id: String,
    _peers: Vec<String>,
    msg_id: usize,
    output: RwLock<BufWriter<Stdout>>,
}

impl MsgHandler<GenerateMessages> for UniqueIdNode {
    fn new(partial_node: glomers::PartialNode) -> Self
    where
        Self: MsgHandler<GenerateMessages>,
    {
        UniqueIdNode {
            id: partial_node.id,
            _peers: partial_node.node_ids,
            msg_id: partial_node.msg_id,
            output: partial_node.output,
        }
    }

    async fn handle_msg(&mut self, msg: Message<GenerateMessages>)
    where
        GenerateMessages: Serialize,
    {
        if matches!(msg.body.msg, GenerateMessages::Generate) {
            self.reply(
                &msg,
                GenerateMessages::GenerateOk {
                    id: format!(
                        "{}-{}-{}",
                        self.id,
                        self.msg_id,
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    ),
                },
            )
            .await
        }
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
    let jh = tokio::spawn(UniqueIdNode::run::<GenerateMessages>());
    jh.await.unwrap();
}

// echo '{"src":"c0","dest":"n3","body":{"type":"init","msg_id":1,"node_id":"n3","node_ids":["n1", "n2", "n3"]}}' | cargo run --bin unique_id
