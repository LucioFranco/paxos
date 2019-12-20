use paxos::{Message, MessageKind, Replica};
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
struct Cluster {
    nodes: HashMap<String, Replica>,
    node_ids: Vec<String>,
    in_flight: VecDeque<(String, String, Message)>,
    client_responses: Vec<String>,
}

impl Cluster {
    pub fn new(size: usize) -> Self {
        let mut nodes = HashMap::new();
        let mut node_ids = Vec::new();

        for i in 0..size {
            let id = format!("{}", i);
            nodes.insert(id.clone(), Replica::new(id.clone(), size));
            node_ids.push(id);
        }

        Self {
            nodes,
            node_ids,
            in_flight: VecDeque::new(),
            client_responses: Vec::new(),
        }
    }

    pub fn propose(&mut self, node: &str, value: String) -> bool {
        let res = self
            .nodes
            .get_mut(node)
            .expect("Invalid node")
            .propose(value);
        self.send_messages(node);
        res
    }

    /// Returns false when there are no more inflight messages
    pub fn step(&mut self) -> bool {
        if let Some((from, to, msg)) = self.in_flight.pop_front() {
            // println!("Message from: {}, to: {}, message: {:?}", from, to, msg);
            let node = self.nodes.get_mut(&to).expect("Invalid node");

            node.step(from.clone(), msg);

            self.send_messages(&to);

            true
        } else {
            false
        }
    }

    pub fn client_responses(&self) -> Vec<String> {
        self.client_responses.clone()
    }

    fn send_messages(&mut self, id: &str) {
        let node = self.nodes.get_mut(id).expect("Invalid node");

        for msg in node.msg_drain() {
            match msg {
                MessageKind::Unary(to, message) => {
                    self.in_flight.push_back((id.to_string(), to, message))
                }
                MessageKind::Broadcast(msg) => {
                    for node in &self.node_ids {
                        if node != id {
                            self.in_flight
                                .push_back((id.to_string(), node.clone(), msg.clone()));
                        }
                    }
                }
                MessageKind::ProposalAccepted(value) => self.client_responses.push(value),
            }
        }
    }
}

#[test]
fn propose() {
    let mut c = Cluster::new(3);

    c.propose("1".into(), "v1".into());

    while c.step() {}

    c.propose("2".into(), "v2".into());

    while c.step() {}

    assert_eq!(
        c.client_responses(),
        vec!["v1".to_string(), "v2".to_string()]
    );
}
