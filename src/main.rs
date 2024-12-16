use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct InitBody {
    node_id: String,
    #[serde(rename(deserialize="node_ids", serialize="node_ids"))]
    peers: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag="type", rename_all="snake_case")]
enum MsgType {
    Init(InitBody),
    InitOk,
    Echo{echo: String},
    EchoOk{echo: String},
}

#[derive(Serialize, Deserialize, Debug)]
struct Body {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    msg: MsgType
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    #[serde(rename(deserialize="dest", serialize="dest"))]
    dst: String,
    body: Body,
}

impl Message {
    fn response(&self, body: Body) -> Message {
        Message{src: self.dst.clone(), dst: self.src.clone(), body}
    }
}

struct Node {
    id: String,
    peers: Vec<String>,
    msg_id: usize,
    input: Lines<BufReader<Stdin>>,
    output: BufWriter<Stdout>
}

impl Node {
    async fn new() -> Node {
        let mut input = BufReader::new(stdin()).lines();
        let output = BufWriter::new(stdout());

        let init_msg: Message = serde_json::from_str(&input.next_line().await.unwrap().unwrap()).unwrap();
        
        let mut node = match init_msg.body.msg {
            MsgType::Init(ref init_body) => {
                Node{id: init_body.node_id.clone(), peers: init_body.peers.clone(), msg_id: 0, input, output}
            },
            _ => {panic!("Should recieve Init message first!")}
        };
        
        node.reply(&init_msg, MsgType::InitOk,).await;
        node
        
    }

    async fn reply(&mut self, msg: &Message, rsp: MsgType) {
        let rsp_body = Body{ msg_id: Some(self.msg_id), in_reply_to: msg.body.msg_id, msg: rsp };
        self.msg_id += 1;
        self.send_msg(msg.response(rsp_body)).await;
    }

    async fn send_msg(&mut self, msg: Message) {
        self.output.write(serde_json::to_string(&msg).unwrap().as_bytes()).await.unwrap();
        self.output.write(b"\n").await.unwrap();
        self.output.flush().await.unwrap();
    }

    async fn handle_msg(&mut self, msg: Message) {
        match msg.body.msg {
            MsgType::Init(_init_body) => panic!("Already initialized"),
            MsgType::InitOk => panic!("Shouldn't be recieving panic OK messages"),
            MsgType::Echo { ref echo } => {
                self.reply(&msg, MsgType::EchoOk { echo: echo.to_string() }).await
            },
            MsgType::EchoOk { echo: _ } => (),
        }
    }

    async fn run(mut self) {
        while let Ok(Some(line)) = self.input.next_line().await {
            self.handle_msg(serde_json::from_str(&line).unwrap()).await
        }
    }
}

#[tokio::main]
async fn main() {
    let node = Node::new().await;
    let jh = tokio::spawn(node.run());
    jh.await.unwrap();
}


// {"src":"c0","dest":"n3","body":{"type":"init","msg_id":1,"node_id":"n3","node_ids":["n1", "n2", "n3"]}}