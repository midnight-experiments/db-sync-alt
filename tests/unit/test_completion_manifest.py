"""Real-store completion/restart controls; deterministic provider, no live calls."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from db_sync_alt.verify.completion import validate_completion
from db_sync_alt.contract import Refusal, digest



class RetainedManifestTests(unittest.TestCase):
    def test_exact_frozen_g3_anchor_shape_and_values_are_preserved(self):
        from db_sync_alt.verify.completion import prepare_finalization
        fixture=json.loads((Path(__file__).resolve().parents[1]/'fixtures/data/completion-g3-manifest.json').read_text())
        g=fixture['generation'];original=copy.deepcopy(g['manifest'])
        self.assertEqual(original['anchors']['start'],{'epoch':276,'height':4500197,'source_id':4500199,'tx_count':3,'hash':'121c92dde3aa55b2b85e5ebd9b6cf9d1f42ad51848584ef6e2fc553f535e2e94'})
        class Store:
            def generation(self,n):return g if n==3 else {'status':'ready','manifest':{'digests':fixture['parent_digests']}}
            def meta(self,key):return original['identity']
            def checkpoint_rows(self,**kw):return []
            # Recorded by a release before epoch parameters: sealed in the nonce-only format.
            def legacy_generation(self,n):return True
            def sealed_tables(self,n):
                from db_sync_alt.contract import LEGACY_TABLES
                return LEGACY_TABLES
        final,latest=prepare_finalization(Store(),3,original,{'fixture':True},original['digests'])
        self.assertIsNone(latest);self.assertEqual(final['completion']['semantic']['anchors'],original['anchors'])
        self.assertEqual({k:v for k,v in final.items() if k!='completion'},original)
        # JSON booleans and integers are different semantic values even though
        # Python equality considers False==0 and True==1.
        g['manifest']=copy.deepcopy(final)
        with patch.object(Store,'checkpoint_rows',side_effect=lambda **kw:[{'unit':'complete','receipt':{'fixture':True}}] if kw.get('phase')=='final-anchors' else []):
            validate_completion(Store(),g,original['digests'],require_seal=True)
            altered=copy.deepcopy(final);altered['completion']['semantic']['capabilities']['bridge_qualified']=0
            altered['completion']['sha256']=digest(altered['completion']['semantic']);g['manifest']=altered
            with self.assertRaisesRegex(Refusal,'completion_mismatch'):validate_completion(Store(),g)
            g['manifest']=copy.deepcopy(final);g['manifest']['capabilities']['bridge_qualified']=0
            with self.assertRaisesRegex(Refusal,'completion_invalid'):validate_completion(Store(),g)
            g['manifest']=copy.deepcopy(final);altered=copy.deepcopy(final);altered['completion']['version']=True
            with self.assertRaisesRegex(Refusal,'completion_mismatch'):validate_completion(Store(),g,manifest=altered)
        g['manifest']=copy.deepcopy(original)
        for value in (True,-1,'276'):
            changed=copy.deepcopy(g);changed['manifest']['anchors']['start']['epoch']=value
            with patch.object(Store,'generation',side_effect=lambda n:changed if n==3 else {'status':'ready','manifest':{'digests':fixture['parent_digests']}}):
                with self.assertRaisesRegex(Refusal,'completion_invalid'):prepare_finalization(Store(),3,changed['manifest'],{},original['digests'])

if __name__=='__main__':unittest.main(verbosity=2)
