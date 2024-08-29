# Day 1

## Why doesn't the memtable provide a delete API?

Because the memtable is write-only, to delete a key, just mask its value with
an empty `&[u8]`

## Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist?

## Why do we need a combination of state and state_lock? Can we only use state.read() and state.write()?

## Why does the order to store and to probe the memtables matter? If a key appears in multiple memtables, which version should you return to the user?

## Is the memory layout of the memtable efficient / does it have good data locality? (Think of how Byte is implemented and stored in the skiplist...) What are the possible optimizations to make the memtable more efficient?

## So we are using parking_lot locks in this tutorial. Is its read-write lock a fair lock? What might happen to the readers trying to acquire the lock if there is one writer waiting for existing readers to stop?

## After freezing the memtable, is it possible that some threads still hold the old LSM state and wrote into these immutable memtables? How does your solution prevent it from happening?

## There are several places that you might first acquire a read lock on state, then drop it and acquire a write lock (these two operations might be in different functions but they happened sequentially due to one function calls the other). How does it differ from directly upgrading the read lock to a write lock? Is it necessary to upgrade instead of acquiring and dropping and what is the cost of doing the upgrade?