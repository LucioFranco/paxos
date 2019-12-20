use paxos::{Message, MessageKind, Replica};

#[test]
fn single() {
    let mut replica = Replica::new("1".into(), 1);

    replica.propose("v1".into());

    assert_eq!(
        replica.msg_drain()[0],
        MessageKind::Broadcast(Message::Prepare { ballot: 1 })
    );

    replica.step("2".into(), Message::Promise { ballot: 1 });

    assert_eq!(
        replica.msg_drain()[0],
        MessageKind::Broadcast(Message::Propose {
            ballot: 1,
            slot: 1,
            value: "v1".into()
        })
    );

    replica.step("2".into(), Message::Accept { ballot: 1 });

    assert_eq!(replica.log_ref().len(), 1);
    assert_eq!(replica.log_ref()[&1], "v1".to_string());
}
