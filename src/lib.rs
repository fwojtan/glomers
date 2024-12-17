use tokio::io::{
    stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct InitBody {
    node_id: String,
    #[serde(rename(deserialize = "node_ids", serialize = "node_ids"))]
    peers: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitMessages {
    Init(InitBody),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Body<M> {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub msg: M,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<M> {
    src: String,
    #[serde(rename(deserialize = "dest", serialize = "dest"))]
    dst: String,
    pub body: Body<M>,
}

impl<M> Message<M> {
    fn response(&self, body: Body<M>) -> Message<M> {
        Message {
            src: self.dst.clone(),
            dst: self.src.clone(),
            body,
        }
    }
}

pub struct PartialNode {
    pub id: String,
    pub peers: Vec<String>,
    pub msg_id: usize,
    pub input: Lines<BufReader<Stdin>>,
    pub output: BufWriter<Stdout>,
}

pub trait MsgHandler<Msg>
where
    Msg: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new Message handling node from the data gathered from the init message.
    /// In the simplest case, this is just returning the same data in a new struct.
    fn new(partial_node: PartialNode) -> Self
    where
        Self: MsgHandler<Msg> + Sized;

    /// The core message-handling logic of a node in the distributed system.
    async fn handle_msg(&mut self, msg: Message<Msg>)
    where
        Msg: Serialize;

    fn get_msg_id(&mut self) -> &mut usize;

    fn get_input(&mut self) -> &mut Lines<BufReader<Stdin>>;

    fn get_output(&mut self) -> &mut BufWriter<Stdout>;

    async fn run<M>()
    where
        M: Serialize + for<'de> Deserialize<'de>,
        Self: Sized,
    {
        let mut input = BufReader::new(stdin()).lines();
        let output = BufWriter::new(stdout());

        let init_msg: Message<InitMessages> =
            serde_json::from_str(&input.next_line().await.unwrap().unwrap()).unwrap();

        let node = match init_msg.body.msg {
            InitMessages::Init(ref init_body) => PartialNode {
                id: init_body.node_id.clone(),
                peers: init_body.peers.clone(),
                msg_id: 0,
                input,
                output,
            },
            _ => {
                panic!("Should recieve Init message first!")
            }
        };

        let mut node = Self::new(node);

        node.reply(&init_msg, InitMessages::InitOk).await;

        // Run in a loop over all input
        while let Ok(Some(line)) = node.get_input().next_line().await {
            node.handle_msg(serde_json::from_str(&line).unwrap()).await;
        }
    }

    async fn reply<M>(&mut self, msg: &Message<M>, rsp: M)
    where
        M: Serialize,
    {
        let rsp_body = Body {
            msg_id: Some(*self.get_msg_id()),
            in_reply_to: msg.body.msg_id,
            msg: rsp,
        };
        *self.get_msg_id() += 1;
        self.send_msg(msg.response(rsp_body)).await;
    }

    async fn send_msg<M>(&mut self, msg: Message<M>)
    where
        M: Serialize,
    {
        let output = self.get_output();
        output
            .write_all(serde_json::to_string(&msg).unwrap().as_bytes())
            .await
            .unwrap();
        output.write_all(b"\n").await.unwrap();
        output.flush().await.unwrap();
    }
}
