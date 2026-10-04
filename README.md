# NODIKV by [nodeicide](https://nodeicide.com)

### K/V Distributed Database

Current version uses iroh to connect the nodes. You must run the bootstrap and copy the endpoint ID into `node.rs`'s ID.

When you run it, you can choose between two commands:

- `PULL k` — read
- `PUSH k v` — write

> ⚠️ **Not consistent.** This design currently favors availability over consistency.

You can run as many nodes as you want, as long as they connect to the correct bootstrap ID.
