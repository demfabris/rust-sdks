---
livekit: patch
---

Fix room teardown memory retention by clearing E2EE state callbacks during cleanup
and reusing the process-wide WebRTC runtime after room handles are dropped.
