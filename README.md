# xmip-core-transport-azure-event-hubs

Azure Event Hubs transport: a Shared Access Signature over the REST API — send a Stream as one event to a hub or one of its partitions; reading is AMQP, and this transport says so — a hub is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
