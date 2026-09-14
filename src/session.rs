//! The far end: enough of Event Hubs to take one Location's events, and
//! what a test or the playground puts on loopback.
//!
//! Not Event Hubs. One session holds the events sent to every hub it is
//! asked about in memory, by partition, verifies every request's token
//! against one policy, and answers the one call the way the service does
//! — 201 for an event, the XML error with its subcode. A hub here opens
//! with [`PARTITIONS`] partitions as a new hub does; an event sent to the
//! hub rather than to a partition is dealt round them in turn, which is
//! what the service does where no partition key is given.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::time::Duration;

use transport::Arrived;
use transport::error::Result;

use crate::ceiling;
use http::message::{Request, Response};
use http::namespace::{self, subcode};
use http::sas::{self, Signer, Token};
use http::server;

/// How many partitions a hub opens with.
pub const PARTITIONS: u32 = 4;

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client sent an event; here is the Stream, its origin the
    /// partition it landed on and the sequence number it was given.
    Sent(Arrived),
    /// The client was answered with this error subcode.
    Refused(String),
}

pub struct Session {
    signer: Signer,
    hubs: BTreeMap<String, Vec<Arrived>>,
    next: u64,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests whose token `policy` made with `key`.
    #[must_use]
    pub fn new(policy: &str, key: &str) -> Self {
        Self {
            signer: Signer::new(policy, key),
            hubs: BTreeMap::new(),
            next: 1,
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Every event taken so far, in the order they came, each under its
    /// origin.
    #[must_use]
    pub fn events(&self) -> Vec<Arrived> {
        let mut all: Vec<Arrived> = self.hubs.values().flatten().cloned().collect();
        all.sort_by_key(|event| {
            event
                .origin_uri
                .rsplit('#')
                .next()
                .and_then(|sequence| sequence.parse::<u64>().ok())
        });
        all
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        let token = match self.signer.verify(request, sas::now()) {
            Ok(token) => token,
            Err(failure) => return refused(401, &failure.message),
        };
        let path = request.path.strip_prefix('/').unwrap_or(&request.path);
        let Some(target) = path.strip_suffix("/messages") else {
            return refused(404, "40400: Not a hub's messages");
        };
        if request.method != "POST" {
            return refused(405, "40500: An event is sent, nothing else");
        }
        let (hub, partition) = match self.place(target) {
            Ok(placed) => placed,
            Err(detail) => return refused(404, detail),
        };
        if request.body.len() > ceiling() {
            return refused(
                413,
                &format!("40000: An event is at most {} bytes", ceiling()),
            );
        }
        let sequence = self.next;
        self.next += 1;
        let origin = format!("{}/{hub}/partitions/{partition}#{sequence}", base(&token));
        let arrived = Arrived::new(origin, request.body.clone());
        self.hubs
            .entry(hub.to_string())
            .or_default()
            .push(arrived.clone());
        (Event::Sent(arrived), Response::new(201))
    }

    /// The hub and partition `target` names — the partition the target
    /// chose where it chose one, the next in turn where it did not.
    fn place<'a>(&self, target: &'a str) -> std::result::Result<(&'a str, u32), &'static str> {
        let Some((hub, partition)) = target.split_once("/partitions/") else {
            let dealt = u32::try_from((self.next - 1) % u64::from(PARTITIONS));
            return Ok((target, dealt.unwrap_or(0)));
        };
        match partition.parse::<u32>() {
            Ok(partition) if partition < PARTITIONS => Ok((hub, partition)),
            _ => Err("40400: No such partition"),
        }
    }
}

/// The token's scheme and authority, which is where the hub lives.
fn base(token: &Token) -> &str {
    token
        .resource
        .split_once("://")
        .map_or(token.resource.as_str(), |(scheme, rest)| {
            &token.resource[..scheme.len() + 3 + rest.find('/').unwrap_or(rest.len())]
        })
}

fn refused(status: u16, detail: &str) -> (Event, Response) {
    (
        Event::Refused(subcode(detail)),
        namespace::error(status, detail),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESOURCE: &str = "http://ns.local/telemetry";

    fn signed(method: &str, path: &str) -> Request {
        Signer::new("policy", "secret").sign(
            Request::new(method, path).header("Host", "ns.local"),
            RESOURCE,
            sas::now() + 60,
        )
    }

    #[test]
    fn a_session_deals_events_round_the_partitions_and_refuses_what_is_not_one() {
        let mut session = Session::new("policy", "secret");
        for expected in 0..=PARTITIONS {
            let (event, response) =
                session.answer(&signed("POST", "/telemetry/messages").body(b"e"));
            assert_eq!(response.status, 201);
            let partition = expected % PARTITIONS;
            let sequence = expected + 1;
            assert_eq!(
                event,
                Event::Sent(Arrived::new(
                    format!("http://ns.local/telemetry/partitions/{partition}#{sequence}"),
                    b"e".to_vec()
                ))
            );
        }
        let chosen = signed("POST", "/telemetry/partitions/3/messages").body(b"");
        let (event, _) = session.answer(&chosen);
        assert_eq!(
            event,
            Event::Sent(Arrived::new(
                "http://ns.local/telemetry/partitions/3#6",
                Vec::new()
            ))
        );
        assert_eq!(session.events().len(), 6);
        assert!(session.events()[5].origin_uri.ends_with("/3#6"));
        let (event, response) = session.answer(&signed("POST", "/telemetry/partitions/9/messages"));
        assert_eq!(
            (event, response.status),
            (Event::Refused("40400".to_string()), 404)
        );
        let (_, response) = session.answer(&signed("POST", "/telemetry/partitions/x/messages"));
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&signed("GET", "/telemetry/messages"));
        assert_eq!(response.status, 405);
        let (_, response) = session.answer(&signed("POST", "/telemetry"));
        assert_eq!(response.status, 404);
        let over = signed("POST", "/telemetry/messages").body(&vec![0; ceiling() + 1]);
        let (event, response) = session.answer(&over);
        assert_eq!(
            (event, response.status),
            (Event::Refused("40000".to_string()), 413)
        );
        let wrong = Signer::new("policy", "wrong").sign(
            Request::new("POST", "/telemetry/messages"),
            RESOURCE,
            sas::now() + 60,
        );
        let (event, response) = session.answer(&wrong);
        assert_eq!(
            (event, response.status),
            (Event::Refused("40103".to_string()), 401)
        );
    }
}
