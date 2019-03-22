# Improvements

- Connection responsible to tell the collection to increase/decrease count to start/stop threads
- Stopping already stopped Scanner/Thread should return Err
- Decrease cloning where possible

# New Features

- Add ability to switch Collection & drop it in the Client
- Add Versioned Tags & Optimistic Locking
- Support batch writes
- Evaluate whether Named Tags are necessary (For Indexes & Projections)
- Build non-blocking Server
- Build non-blocking Client
- Consider creating an Indexer thread to persist updated indexes without slowing down writes

# Tests

- Add Tests for Subscription moving from Scanner to Publisher (Should NOT skip Events)
- Add Tests for Publisher including one that checks inactive subscriptions are cleaned up
- Add Query Test for Interval
- Improve Logger Tests
- Improve Config::load Tests
- Review Tests

# Docs

- Add Publisher Docs
- Double-check Docs after changes
- Architecture Docs