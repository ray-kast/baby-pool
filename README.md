# `topograph` - A tiny thread pool and toposort implementation
No diving allowed!

**WARNING:** The `0.2.x-alpha*` versions use the `generic_associated_types`
feature gate.  I apologize.

This is a quick and dirty thread-scheduling library I wrote up for handling
non-async jobs in a concurrent queue.  For this specific use case, `rayon` and
`tokio`, which are both fantastic packages, didn't quite do what I wanted.

For more info on this package, check out [the docs](https://docs.rs/topograph).
