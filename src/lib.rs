use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct Replica {
    ballot: usize,
    slot: usize,
    log: HashMap<usize, String>,

    id: String,
    num_nodes: usize,

    state: State,
    outbound_messages: VecDeque<MessageKind>,
}

// #[derive(Debug)]
// struct Entry {
//     ballot: usize,
//     slot: usize,
//     value: String,
// }

#[derive(Debug, Eq, PartialEq)]
pub enum MessageKind {
    Unary(String, Message),
    Broadcast(Message),
    ProposalAccepted(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Prepare {
        ballot: usize,
    },
    Promise {
        ballot: usize,
    },
    Propose {
        ballot: usize,
        slot: usize,
        value: String,
    },
    Accept {
        ballot: usize,
    },
}

#[derive(Debug)]
enum State {
    Idle,
    Phase1(Quorum, String),
    Phase2(Quorum, String),
}

impl Replica {
    pub fn new(id: String, num_nodes: usize) -> Self {
        Self {
            ballot: 0,
            slot: 0,
            log: HashMap::new(),
            id,
            num_nodes,
            state: State::Idle,
            outbound_messages: VecDeque::new(),
        }
    }

    // TODO: do we want to reject proposals until this one is done?
    pub fn propose(&mut self, value: String) -> bool {
        if let State::Idle = self.state {
            self.ballot += 1;

            let mut quorum = Quorum::new(self.num_nodes);
            quorum.ack(self.id.clone());
            self.state = State::Phase1(quorum, value);

            let message = Message::Prepare {
                ballot: self.ballot,
            };
            self.outbound_messages
                .push_back(MessageKind::Broadcast(message));

            true
        } else {
            false
        }
    }

    pub fn step(&mut self, from: String, msg: Message) {
        match msg {
            Message::Prepare { ballot } => {
                if ballot > self.ballot {
                    // TODO: select a correct ballot
                    let ballot = ballot;
                    let promise = Message::Promise { ballot };

                    self.outbound_messages
                        .push_back(MessageKind::Unary(from, promise));
                }
            }

            Message::Promise { ballot } => {
                // Only answer this if are currently proposing
                if let State::Phase1(quorum, value) = &mut self.state {
                    // Other ballot is bigger so reset
                    if ballot > self.ballot {
                        self.state = State::Idle;
                        todo!("re-propose")
                    }

                    if ballot == self.ballot {
                        quorum.ack(from);

                        // If quorum, start Phase2
                        if quorum.is_majority() {
                            self.slot += 1;

                            let mut quorum = Quorum::new(self.num_nodes);
                            quorum.ack(self.id.clone());

                            let accept_req = Message::Propose {
                                ballot,
                                slot: self.slot,
                                value: value.clone(),
                            };

                            self.outbound_messages
                                .push_back(MessageKind::Broadcast(accept_req));

                            self.log.insert(self.slot, value.clone());
                            self.state = State::Phase2(quorum, value.clone());
                        }
                    }
                }
            }

            Message::Propose {
                ballot,
                slot,
                value,
            } => {
                if ballot >= self.ballot {
                    self.ballot = ballot;
                    self.log.insert(slot, value.clone());

                    let accept = Message::Accept { ballot };
                    self.outbound_messages
                        .push_back(MessageKind::Unary(from.clone(), accept));
                }
            }

            Message::Accept { ballot: _ } => {
                if let State::Phase2(quorum, _) = &mut self.state {
                    quorum.ack(from.clone());

                    if quorum.is_majority() {
                        self.state = State::Idle;

                        let value = self.log[&self.slot].clone();

                        self.outbound_messages
                            .push_back(MessageKind::ProposalAccepted(value));
                        // TODO: do we want to commit?
                    }
                }
            }
        }
    }

    pub fn msg_drain(&mut self) -> Vec<MessageKind> {
        self.outbound_messages.drain(..).collect()
    }

    pub fn log_ref(&self) -> &HashMap<usize, String> {
        &self.log
    }
}

#[derive(Debug)]
struct Quorum {
    size: usize,
    total_nodes: usize,
    acks: HashMap<String, bool>,
    // nacks: HashMap<usize, bool>,
}

impl Quorum {
    pub fn new(total_nodes: usize) -> Self {
        Self {
            size: 0,
            total_nodes,
            acks: HashMap::new(),
            // nacks: HashMap::new(),
        }
    }

    pub fn ack(&mut self, id: String) {
        self.acks.insert(id, true);
    }

    pub fn is_majority(&self) -> bool {
        self.acks.len() > self.total_nodes / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn majority_quorum() {
        let mut quorum = Quorum::new(3);

        quorum.ack("1".into());
        quorum.ack("2".into());

        assert!(quorum.is_majority());
    }
}
