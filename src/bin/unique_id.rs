use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{BufReader, BufWriter, Lines, Stdin, Stdout};

use glomers::{Message, MsgHandler};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GenerateMessages {
    Generate,
    GenerateOk{id: String},
}

struct UniqueIdNode {
    id: String,
    _peers: Vec<String>,
    msg_id: usize,
    input: Lines<BufReader<Stdin>>,
    output: BufWriter<Stdout>,
}

impl MsgHandler<GenerateMessages> for UniqueIdNode {
    fn new(partial_node: glomers::PartialNode) -> Self
    where
        Self: MsgHandler<GenerateMessages>,
    {
        UniqueIdNode {
            id: partial_node.id,
            _peers: partial_node.peers,
            msg_id: partial_node.msg_id,
            input: partial_node.input,
            output: partial_node.output,
        }
    }

    async fn handle_msg(&mut self, msg: Message<GenerateMessages>)
    where
        GenerateMessages: Serialize,
    {
        if matches!(msg.body.msg, GenerateMessages::Generate){
            self.reply(
                &msg,
                GenerateMessages::GenerateOk {
                    id: format!("{}-{}-{}", self.id, self.msg_id, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())
                },
            )
            .await
        }
    }

    fn get_msg_id(&mut self) -> &mut usize {
        &mut self.msg_id
    }

    fn get_input(&mut self) -> &mut Lines<BufReader<Stdin>> {
        &mut self.input
    }

    fn get_output(&mut self) -> &mut BufWriter<Stdout> {
        &mut self.output
    }
}

#[tokio::main]
async fn main() {
    let jh = tokio::spawn(UniqueIdNode::run::<GenerateMessages>());
    jh.await.unwrap();
}
