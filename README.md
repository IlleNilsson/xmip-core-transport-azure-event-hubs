# xmip-core-transport-azure-event-hubs

Azure Event Hubs transport: a Shared Access Signature over the REST API — send a Stream as one event to a hub or one of its partitions; reading is AMQP, and this transport says so — a hub is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location sends to the hub, and the partition, its `hub` and `partition` settings name: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which it reads, through `transport::destination::named`, as those, its address naming only the namespace. A target naming another hub — `alarms`, `alarms/partitions/3` — still wins (ADR-0064, amendment 2026-11-10).

A Location signs under the shared access policy its `key_name` setting names, `RootManageSharedAccessKey` or one of its own, with the policy's key its `secret = "<name>"` names: a `password` secret kept in the Administration Database and never in configuration, read at each request, so a key set again reaches the next send without a restart (ADR-0019, amendment 2026-10-29; ADR-0015, amendment 2026-11-25). A Location naming a secret and no `key_name` is refused as the node starts: a Shared Access Signature names its policy.

The Shared Access Signature and the judgment of a namespace's answers come from [xmip-core-transport-azure](https://github.com/IlleNilsson/xmip-core-transport-azure), where every Azure technology shares what Azure speaks over HTTP (ADR-0044, amendment 2026-09-24); HTTP itself comes from [xmip-core-transport-http](https://github.com/IlleNilsson/xmip-core-transport-http).

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

The namespace an origin names is `azure::sas::Token::namespace`; until 2026-09-28 this technology and Service Bus each cut a token's resource by hand.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `key_name`, `hub`, `partition` and `timeout`, and `SCHEMES`, `azure-event-hubs`; `key_name` is the Azure crate's, which this crate's declaration takes by that crate's own `declaration` feature. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the http technology, and with it TLS — are behind the default feature, and `tls`, which is code, turns the default on with it. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check an Event Hubs Location's settings though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
