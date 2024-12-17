use tokio::io::{BufReader, BufWriter, Lines, Stdin, Stdout};

use glomers::{Message, MsgHandler};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoMessages {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    _id: String,
    _peers: Vec<String>,
    msg_id: usize,
    input: Lines<BufReader<Stdin>>,
    output: BufWriter<Stdout>,
}

impl MsgHandler<EchoMessages> for EchoNode {
    fn new(partial_node: glomers::PartialNode) -> Self
    where
        Self: MsgHandler<EchoMessages>,
    {
        EchoNode {
            _id: partial_node.id,
            _peers: partial_node.peers,
            msg_id: partial_node.msg_id,
            input: partial_node.input,
            output: partial_node.output,
        }
    }

    async fn handle_msg(&mut self, msg: Message<EchoMessages>)
    where
        EchoMessages: Serialize,
    {
        if let EchoMessages::Echo { ref echo } = msg.body.msg {
            self.reply(
                &msg,
                EchoMessages::EchoOk {
                    echo: echo.to_string(),
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
    let jh = tokio::spawn(EchoNode::run::<EchoMessages>());
    jh.await.unwrap();
}

// echo {"src":"c0","dest":"n3","body":{"type":"init","msg_id":1,"node_id":"n3","node_ids":["n1", "n2", "n3"]}} | cargo run --bin echo
