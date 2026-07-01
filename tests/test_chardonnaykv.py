import unittest

from chardonnaykv import ChardonnayKV, transfer


class ChardonnayKVTests(unittest.TestCase):
    def test_snapshot_remains_stable_after_later_commit(self) -> None:
        kv = ChardonnayKV(shard_count=4, replica_count=3)
        kv.put("alice", 100)
        kv.put("bob", 25)

        snapshot = kv.snapshot()
        kv.transaction(transfer("alice", "bob", 40))

        self.assertEqual(snapshot.items(["alice", "bob"]), {"alice": 100, "bob": 25})
        self.assertEqual(kv.snapshot().items(["alice", "bob"]), {"alice": 60, "bob": 65})

    def test_dry_run_reports_read_write_sets_and_prefetches(self) -> None:
        kv = ChardonnayKV(shard_count=4, replica_count=3)
        kv.put("alice", 100)
        kv.put("bob", 25)

        dry = kv.dry_run(transfer("alice", "bob", 30))

        self.assertEqual(dry.read_keys, ("alice", "bob"))
        self.assertEqual(dry.write_keys, ("alice", "bob"))
        self.assertEqual(dry.prefetched, {"alice": 100, "bob": 25})

    def test_commits_are_replicated_to_participant_logs(self) -> None:
        kv = ChardonnayKV(shard_count=4, replica_count=3)
        kv.put("alice", 100)
        kv.put("bob", 25)
        kv.transaction(transfer("alice", "bob", 10))

        log_entries = [record.entry for shard in kv.shards for record in shard.log]

        self.assertGreaterEqual(len(log_entries), 3)
        self.assertTrue(any(entry.writes for entry in log_entries))


if __name__ == "__main__":
    unittest.main()
