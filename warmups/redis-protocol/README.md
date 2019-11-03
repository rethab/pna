# Simple Redis Client with Echo Server

## Supported commands (tested with real server)

* PING
* GET
* SET

## Sample
### Dummy Server
`cargo run --bin server` (starts on port 6379)
### Redis in Docker
`docker run -d -p 6379:6379 --name redis1 redis`
### Client
* PING: `cargo run --bin client -- --server localhost:6379 ping`
* SET: `cargo run --bin client -- --server localhost:6379 set mykey myvalue`
* GET: `cargo run --bin client -- --server localhost:6379 get mykey`
