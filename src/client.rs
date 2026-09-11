//! Xmip's side: the one call a Send Location makes, a request with a
//! Shared Access Signature over one connection to the namespace.
//!
//! A hub is a path under the namespace — `https://ns.servicebus.windows.
//! net/telemetry` in the cloud, `http://127.0.0.1:port/telemetry` for a
//! stand-in — and one event is one `POST …/messages`, or `POST …/
//! partitions/<id>/messages` where the Location chooses the partition
//! rather than letting the hub. The body is the event; the REST API
//! documents `application/atom+xml;type=entry;charset=utf-8` as its
//! content type and takes any bytes under it.

use std::time::Duration;

use transport::error::Result;

use http::endpoint;
use http::message::{self, Request, Response};
use transport_azure_service_bus::rest;
use transport_azure_service_bus::sas::{self, Signer};

/// The content type the REST API documents for one event.
pub const CONTENT_TYPE: &str = "application/atom+xml;type=entry;charset=utf-8";

pub struct Client {
    endpoint: String,
    host: String,
    signer: Signer,
    timeout: Option<Duration>,
}

impl Client {
    /// Speak to the namespace at `endpoint` — `http://host:port` or
    /// `https://host:port` — signing as `policy` with `key`.
    ///
    /// # Errors
    /// Where `endpoint` is not an HTTP URL.
    pub fn new(endpoint: &str, policy: &str, key: &str) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            host: endpoint::authority(endpoint)?,
            signer: Signer::new(policy, key),
            timeout: None,
        })
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The resource a token for `hub` names: the hub's own URL.
    #[must_use]
    pub fn resource(&self, hub: &str) -> String {
        format!("{}/{hub}", self.endpoint)
    }

    /// Send `bytes` as one event to `hub`, to `partition` where one is
    /// named and to whichever the hub chooses where none is.
    ///
    /// # Errors
    /// Where the namespace refused or could not be reached.
    pub fn send(&self, hub: &str, partition: Option<&str>, bytes: &[u8]) -> Result<()> {
        let path = match partition {
            Some(partition) => format!("/{hub}/partitions/{partition}/messages"),
            None => format!("/{hub}/messages"),
        };
        let request = Request::new("POST", path)
            .header("Content-Type", CONTENT_TYPE)
            .header("Host", &self.host)
            .body(bytes);
        let expiry = sas::now() + sas::LIFETIME;
        let signed = self.signer.sign(request, &self.resource(hub), expiry);
        let stream = endpoint::connect(&self.endpoint, self.timeout)?;
        rest::judge("Event Hubs", message::exchange(stream, &signed)?).map(|_: Response| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use transport::Arrived;
    use transport::socket;

    #[test]
    fn an_event_reaches_a_session_at_the_hub_or_at_one_of_its_partitions() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session =
                Session::new("policy", "secret").timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..3)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let client = Client::new(&format!("http://{address}/"), "policy", "secret")
            .expect("endpoint")
            .timing_out_after(Duration::from_secs(2));
        client.send("telemetry", None, b"UNA:+.? '").expect("sent");
        client
            .send("telemetry", Some("3"), &[0, 0xff, b'\n'])
            .expect("sent");
        let refused = client
            .send("telemetry", Some("32"), b"x")
            .expect_err("no such partition");
        assert!(refused.message.contains("404"), "{refused}");
        assert!(!refused.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.events().len(), 2);
        let base = format!("http://{address}/telemetry/partitions");
        assert_eq!(
            events[0],
            Event::Sent(Arrived::new(format!("{base}/0#1"), b"UNA:+.? '".to_vec()))
        );
        assert_eq!(
            events[1],
            Event::Sent(Arrived::new(format!("{base}/3#2"), vec![0, 0xff, b'\n']))
        );
        assert_eq!(events[2], Event::Refused("40400".to_string()));
        assert_eq!(
            client.resource("telemetry"),
            format!("http://{address}/telemetry")
        );
    }

    #[test]
    fn a_missing_endpoint_is_retryable_and_a_malformed_one_is_not() {
        let nobody = Client::new("http://127.0.0.1:1", "p", "k").expect("ok");
        assert!(nobody.send("h", None, b"x").expect_err("nobody").retryable);
        assert!(Client::new("ns.local", "p", "k").is_err());
    }
}
