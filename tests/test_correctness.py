import unittest
from lab.exercises import apply_ordered_events,replay_experiment,temporal_features,governed_retrieval
class Correctness(unittest.TestCase):
    def test_replay_delete_and_conflict(self):
        self.assertEqual(replay_experiment()['late_update_after_delete'],'passed')
    def test_point_in_time(self):
        result=temporal_features()
        self.assertEqual(result['point_in_time_feature'],10)
        self.assertEqual(result['event_time_only_leaky_feature'],100)
    def test_restricted_evidence(self):
        self.assertEqual(governed_retrieval()['public_prompt_document_ids'],['public-1'])
if __name__=='__main__':unittest.main()
