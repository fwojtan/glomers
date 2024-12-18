use tokio::{
    io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdout},
    sync::{mpsc, RwLock},
    time::sleep,
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
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub msg: M,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<M> {
    pub src: String,
    #[serde(rename(deserialize = "dest", serialize = "dest"))]
    pub dst: String,
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

#[derive(Debug)]
enum Events<M>
where
    M: Send,
{
    ReceivedMsg(Message<M>),
    TriggerBgTask,
}

pub struct PartialNode {
    pub id: String,
    pub peers: Vec<String>,
    pub msg_id: usize,
    pub output: RwLock<BufWriter<Stdout>>,
}

pub trait MsgHandler<Msg>
where
    Msg: Serialize + for<'de> Deserialize<'de> + Send + 'static,
{
    /// Create a new Message handling node from the data gathered from the init message.
    /// In the simplest case, this is just returning the same data in a new struct.
    fn new(partial_node: PartialNode) -> Self
    where
        Self: MsgHandler<Msg> + Sized;

    /// The core message-handling logic of a node in the distributed system.
    async fn handle_msg(&mut self, msg: Message<Msg>)
    where
        Msg: Serialize + Send;

    /// Background task. A recurring event at a fixed interval that you can write
    /// arbitrary code in. Set the frequency using the 'bg_task_interval_ms' method.
    async fn bg_task(&mut self) {
        ()
    }

    /// How frequently the 'bg_task' method should be called. Leave as u64::MAX
    /// if you don't want to do anything in the background
    fn bg_task_interval_ms(&self) -> u64 {
        u64::MAX
    }

    fn get_msg_id(&mut self) -> &mut usize;

    fn get_output(&self) -> &RwLock<BufWriter<Stdout>>;

    /// Initializes a node and runs tasks to both parse the input stream and regularly trigger
    /// background events using a rudimentary event handler.
    async fn run<M>()
    where
        M: Serialize + for<'de> Deserialize<'de>,
        Self: Sized,
    {
        let mut input = BufReader::new(stdin()).lines();
        let output = RwLock::new(BufWriter::new(stdout()));

        let init_msg: Message<InitMessages> =
            serde_json::from_str(&input.next_line().await.unwrap().unwrap()).unwrap();

        let node = match init_msg.body.msg {
            InitMessages::Init(ref init_body) => PartialNode {
                id: init_body.node_id.clone(),
                peers: init_body.peers.clone(),
                msg_id: 0,
                output,
            },
            _ => {
                panic!("Should recieve Init message first!")
            }
        };

        let mut node = Self::new(node);

        node.reply(&init_msg, InitMessages::InitOk).await;

        let (tx, mut rx) = mpsc::channel(10);
        let tx2 = tx.clone();

        tokio::spawn(async move {
            loop {
                match input.next_line().await {
                    Ok(Some(line)) => {
                        let message = serde_json::from_str(&line).unwrap();
                        tx.send(Events::ReceivedMsg(message))
                            .await
                            .expect("Channel send error from input task");
                    }
                    Err(_) => {
                        panic!("Input IO error");
                    }
                    Ok(None) => break,
                }
            }
        });

        let interval = node.bg_task_interval_ms();

        tokio::spawn(async move {
            loop {
                sleep(tokio::time::Duration::from_millis(interval)).await;
                tx2.send(Events::TriggerBgTask)
                    .await
                    .expect("Channel send error from sleep task");
            }
        });

        while let Some(event) = rx.recv().await {
            match event {
                Events::ReceivedMsg(message) => {
                    node.handle_msg(message).await;
                }
                Events::TriggerBgTask => {
                    node.bg_task().await;
                }
            }
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
        self.send_msg(msg.response(rsp_body)).await;
    }

    async fn send_msg<M>(&mut self, msg: Message<M>)
    where
        M: Serialize,
    {
        *self.get_msg_id() += 1;
        let output = self.get_output();
        self.send_msg_inner(output, msg).await
    }

    /// Allows you to send messages without having to mutably borrow self. Only use as break glass option
    /// CAUTION: doesn't incr the msg_id - you must do that manually if you use this.
    async fn send_msg_inner<M>(&self, output: &RwLock<BufWriter<Stdout>>, msg: Message<M>)
    where
        M: Serialize,
    {
        output
            .write()
            .await
            .write_all(serde_json::to_string(&msg).unwrap().as_bytes())
            .await
            .unwrap();
        output.write().await.write_all(b"\n").await.unwrap();
        output.write().await.flush().await.unwrap();
    }
}
