# multithreaded-web-server

A single-threaded HTTP server upgraded into a multithreaded one using a hand-rolled thread pool — the capstone project from *The Rust Programming Language* book (Ch. 20), built for learning concurrency, `Arc`/`Mutex`, channels, and graceful shutdown.

## What it does

The server listens on `127.0.0.1:7878` and serves static HTML pages based on the request path:

| Request              | Response               |
|-----------------------|-------------------------|
| `GET /`                | `pages/home.html`       |
| `GET /sleep`           | `pages/sleep.html` (after a 5s delay, to simulate a slow request) |
| anything else          | `pages/404.html`        |

Instead of spawning a new OS thread per connection (or handling requests one at a time), incoming connections are dispatched to a fixed pool of worker threads, so a slow `/sleep` request doesn't block other clients.

## Architecture

**`ThreadPool`** (`lib.rs`)
- Holds a fixed set of `Worker`s and an `mpsc::Sender<Job>`.
- `ThreadPool::new(size)` spawns `size` workers sharing one `Arc<Mutex<Receiver<Job>>>`; returns `Err` if `size == 0`.
- `execute(f)` boxes any `FnOnce() + Send + 'static` closure as a `Job` and sends it down the channel.

**`Worker`**
- Wraps a `JoinHandle` and loops on `receiver.lock().recv()`, running whatever job it receives.
- Exits its loop when `recv()` returns `Err` (channel disconnected).

**Graceful shutdown (`Drop for ThreadPool`)**
- Drops the `Sender` first, so all workers' `recv()` calls return `Err` once the queue is drained.
- Joins every worker thread, ensuring in-flight jobs finish before the process exits.

## Project structure

```
.
├── src/
│   ├── main.rs   # TCP listener + request handling
│   └── lib.rs    # ThreadPool / Worker / Job
├── pages/
│   ├── home.html
│   ├── sleep.html
│   └── 404.html
└── Cargo.toml
```

## Running it

```bash
cargo run
```

Then visit:
- http://127.0.0.1:7878/ — home page
- http://127.0.0.1:7878/sleep — delayed response (open a second tab at `/` while this loads to see it isn't blocked)
- http://127.0.0.1:7878/anything — 404 page

## Testing

`lib.rs` includes unit tests for the pool in isolation (no networking):

```bash
cargo test
```

Covers: rejecting a zero-sized pool, running single/many jobs, verifying jobs actually run concurrently (via a `Barrier`), and confirming `Drop` waits for in-flight jobs before returning.

## What this project demonstrates

- Safe shared ownership across threads with `Arc<Mutex<T>>`
- Work distribution via `mpsc` channels
- Bounding concurrency with a fixed-size pool instead of unbounded `thread::spawn`
- Graceful shutdown by closing a channel and joining handles
- Basic raw HTTP handling over `TcpStream` without a web framework

## Notes

- Single-threaded fallback code (`thread::spawn` per connection, and a fully sequential version) is left commented out in `main.rs`/`lib.rs` for comparison — worth reading to see the progression.
- This is a learning project, not production-ready: no HTTP parsing beyond the request line, no keep-alive, no error recovery beyond `expect()`.