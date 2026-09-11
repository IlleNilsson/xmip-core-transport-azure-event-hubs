#![forbid(unsafe_code)]

//! Streams that travel as events into an Event Hub. One event is one
//! Stream, the partition it landed on and its sequence number kept beside
//! it.
//!
//! Event Hubs is the event log of every organisation that lives in Azure:
//! a hub, partitions under it, consumers reading each partition from an
//! offset. Its REST API is one call: send an event to the hub, or to one
//! of its partitions. A Send Location sends a Stream as one event, with a
//! Shared Access Signature over plain HTTP/1.1 on a socket — `https://`
//! with the `tls` feature, which is the http technology's TLS (ADR-0033).
//!
//! **Reading is AMQP.** Event Hubs hands events to a consumer over AMQP
//! 1.0 and nothing else; the REST API sends and does not read. This
//! transport says so: [`Transport::directions`] is send only, and
//! [`Transport::receive`] answers with the reason rather than an empty
//! vector, so a Receive Location configured on it learns at once what it
//! has. The estate's amqp technology is where reading belongs.
//!
//! ```text
//! client.rs    Xmip's side: send to the hub or to a partition
//! session.rs   the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint and HTTP itself come from the http technology; the Shared
//! Access Signature and the judgement of the namespace's answers from the
//! azure-service-bus technology, which signs at the same namespaces and
//! built them for this crate to take (ADR-0044).
//!
//! An event is bytes — the body as it is, one mebibyte at most on the
//! Standard tier: [`ceiling`]. Nothing is refused for its content, an
//! empty body included.
//!
//! A hub is not an artefact anyone claims, so [`Transport::claims`]
//! answers `None`. The origin URI is the partition's URL with the sequence
//! number as its fragment. A send target is a hub name under this
//! transport's namespace, `hub/partitions/<id>` for one of its partitions,
//! or empty for this transport's own hub and partition.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up at the endpoint's authority and takes the one send.

pub mod client;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{CONTENT_TYPE, Client};
use http::endpoint;
pub use session::{Event, PARTITIONS, Session};
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

/// The largest event a Standard namespace carries: one mebibyte.
#[must_use]
pub const fn ceiling() -> usize {
    1024 * 1024
}

/// What the loopback pair agrees on: one hub, one policy and its key.
const LOOPBACK_HUB: &str = "telemetry";
const LOOPBACK_POLICY: &str = "RootManageSharedAccessKey";
const LOOPBACK_KEY: &str = "probe";

#[derive(Clone)]
pub struct EventHubsTransport {
    endpoint: String,
    hub: String,
    partition: Option<String>,
    policy: String,
    key: String,
    timeout: Option<Duration>,
}

impl EventHubsTransport {
    /// Speak to the namespace at `endpoint` — `https://<ns>.servicebus.
    /// windows.net` in the cloud, `http://host:port` for a stand-in —
    /// about `hub`.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, hub: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            hub: hub.to_string(),
            partition: None,
            policy: String::new(),
            key: String::new(),
            timeout: None,
        }
    }

    /// Sign as this shared access policy with its key.
    #[must_use]
    pub fn with_policy(mut self, policy: &str, key: &str) -> Self {
        self.policy = policy.to_string();
        self.key = key.to_string();
        self
    }

    /// Send to this partition of the hub rather than letting the hub
    /// choose.
    #[must_use]
    pub fn on_partition(mut self, partition: &str) -> Self {
        self.partition = Some(partition.to_string());
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport speaks through.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn client(&self) -> Result<Client> {
        let client = Client::new(&self.endpoint, &self.policy, &self.key)?;
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that holds this transport's policy and key, for a test or
    /// the playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.policy, &self.key);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// The hub and partition a target names — `hub`, `hub/partitions/3` —
    /// or this transport's own where it names none.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, Option<&'a str>) {
        if target.is_empty() {
            return (&self.hub, self.partition.as_deref());
        }
        match target.split_once("/partitions/") {
            Some((hub, partition)) => (hub, Some(partition)),
            None => (target, None),
        }
    }
}

impl Transport for EventHubsTransport {
    fn name(&self) -> &'static str {
        "azure-event-hubs"
    }

    fn directions(&self) -> Directions {
        Directions::SEND
    }

    /// Never anything: reading a hub is AMQP, and this transport says so.
    fn receive(&self) -> Result<Vec<Arrived>> {
        Err(TransportError::permanent(
            "reading an Event Hub is AMQP 1.0, which the REST API does not speak; \
             this transport sends, and the amqp technology reads",
        ))
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > ceiling() {
            return Err(TransportError::permanent(format!(
                "{} bytes is over the {} one Event Hubs event carries",
                bytes.len(),
                ceiling()
            )));
        }
        let (hub, partition) = self.resolve(target);
        self.client()?.send(hub, partition, bytes)
    }
}

impl EventHubsTransport {
    /// Both ends on this machine: an ephemeral local port, one policy and
    /// key the far end expects and the near end signs with, the loopback
    /// timeout.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("http://127.0.0.1:0", LOOPBACK_HUB)
            .with_policy(LOOPBACK_POLICY, LOOPBACK_KEY)
            .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

/// A bound session waiting for its one send.
struct Serving {
    session: Session,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Serving {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(mut self: Box<Self>) -> Result<Arrived> {
        match self.session.serve_one(&self.listener)? {
            Event::Sent(arrived) => Ok(arrived),
            Event::Refused(code) => Err(protocol_error(format!("the session refused: {code}"))),
        }
    }
}

impl Loopback for EventHubsTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = socket::bind_tcp(&endpoint::authority(&self.endpoint)?)?;
        Ok(Box::new(Serving {
            session: self.session(),
            listener,
            address,
        }))
    }

    /// Send the payload as one event, from a fresh near end signing as
    /// this transport does, at the namespace on `address`.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self {
            endpoint: format!("http://{address}"),
            ..self.clone()
        };
        near.send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(endpoint: &str, key: &str) -> EventHubsTransport {
        EventHubsTransport::new(endpoint, "telemetry")
            .with_policy("policy", key)
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn what_is_sent_lands_on_the_hub_or_the_partition_the_target_names() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = node(&format!("http://{address}"), "secret").on_partition("2");
        let mut session = near.session();
        let far_end = std::thread::spawn(move || {
            let events: Vec<Event> = (0..3)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        near.send("", b"UNA:+.? '").expect("its own partition");
        near.send("telemetry", b"").expect("the hub chooses");
        near.send("audit/partitions/1", &[0xff])
            .expect("another hub's partition");
        let (session, events) = far_end.join().expect("thread");
        let base = format!("http://{address}");
        assert_eq!(
            events[0],
            Event::Sent(Arrived::new(
                format!("{base}/telemetry/partitions/2#1"),
                b"UNA:+.? '".to_vec()
            ))
        );
        let origin = |event: &Event| match event {
            Event::Sent(arrived) => arrived.origin_uri.clone(),
            Event::Refused(code) => code.clone(),
        };
        assert!(origin(&events[1]).ends_with("/telemetry/partitions/1#2"));
        assert!(origin(&events[2]).ends_with("/audit/partitions/1#3"));
        assert_eq!(session.events().len(), 3);
    }

    #[test]
    fn a_wrong_key_is_refused_with_the_namespaces_own_status_and_subcode() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let mut session = node("http://x", "secret").session();
        let far_end = std::thread::spawn(move || session.serve_one(&listener).expect("served"));
        let failure = node(&format!("http://{address}"), "wrong")
            .send("", b"x")
            .expect_err("refused");
        assert!(failure.message.contains("401 40103"), "{failure}");
        assert!(!failure.retryable);
        assert_eq!(
            far_end.join().expect("thread"),
            Event::Refused("40103".to_string())
        );
    }

    #[test]
    fn a_hub_sends_only_and_says_that_reading_is_amqp() {
        let near = node("http://127.0.0.1:1", "secret");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "azure-event-hubs");
        assert!(near.directions().sends() && !near.directions().receives());
        let reading = near.receive().expect_err("AMQP");
        assert!(reading.message.contains("AMQP"), "{reading}");
        assert!(!reading.retryable);
        assert!(
            near.send("", b"x")
                .expect_err("nothing listening")
                .retryable
        );
        assert!(
            !node("ns.local", "s")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
    }

    #[test]
    fn what_is_over_the_ceiling_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1", "secret");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(failure.message.contains("1048576"), "{failure}");
    }

    #[test]
    fn an_event_rounds_through_the_loopback_session() {
        let loopback = EventHubsTransport::loopback();
        let arrived = loopback.round(b"UNA:+.? '").expect("round");
        assert_eq!(arrived.bytes, b"UNA:+.? '");
        assert!(
            arrived.origin_uri.starts_with("http://127.0.0.1:"),
            "{}",
            arrived.origin_uri
        );
        assert!(
            arrived.origin_uri.contains("/telemetry/partitions/0#1"),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(loopback.name(), "azure-event-hubs");
        assert_eq!(loopback.ceiling(), Some(ceiling()));
        assert!(loopback.refuses(&[0xff]).is_none());
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it, and one at the brim.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'x'; ceiling()]),
        ]
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_over_the_brim() {
        let loopback = EventHubsTransport::loopback();
        for (name, payload) in edge_payloads() {
            assert!(loopback.refuses(&payload).is_none(), "{name}");
            let arrived = loopback.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        let over = vec![b'x'; ceiling() + 1];
        let failure = loopback.round(&over).expect_err("over the brim");
        assert!(failure.message.starts_with("send failed:"), "{failure}");
        assert!(failure.message.contains("1048576"), "{failure}");
    }
}
