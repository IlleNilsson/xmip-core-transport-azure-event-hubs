//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48). `key_name` is the Azure crate's, taken by that crate's
//! own `declaration` feature.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the namespace,
/// `https://<ns>.servicebus.windows.net`. The shared access policy is the
/// `key_name` setting, and its key the Location's `secret`, a password
/// secret the Administration Database keeps, never a setting
/// (`Configured::secured_by`).
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        azure::key_name::key_name(Applies::Send),
        Setting {
            name: "hub",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The Event Hub sent to when a send target names none.",
            applies: Applies::Send,
        },
        Setting {
            name: "partition",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The partition of the hub sent to; the hub chooses when left out.",
            applies: Applies::Send,
        },
        transport::configured::timeout(
            Applies::Send,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["azure-event-hubs"];
