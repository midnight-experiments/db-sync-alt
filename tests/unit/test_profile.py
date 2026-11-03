# SPDX-License-Identifier: Apache-2.0
"""Pinned Preprod's empty observed asset and distinct named auxiliary assets."""
import hashlib
import json
import unittest

from db_sync_alt.resources import profile_dir

PROFILE=profile_dir('preprod')

class PreprodTests(unittest.TestCase):
    def test_profile_matches_pinned_public_hashes_and_official_semantics(self):
        manifest=json.loads((PROFILE/'source-manifest.json').read_text())
        self.assertEqual(manifest['commit'],'3acfd2edecdcd91373b6a506933f4c14ad3f0308')
        self.assertEqual(len(manifest['files']),6)
        for name,entry in manifest['files'].items():
            self.assertEqual(hashlib.sha256((PROFILE/name).read_bytes()).hexdigest(),entry['sha256'])
        cn=json.loads((PROFILE/'cnight-config.json').read_text())
        self.assertEqual(cn['addresses']['cnight_asset_name'],'')
        self.assertEqual(cn['next_cardano_position']['block_number'],4500197)
        self.assertEqual(cn['next_cardano_position']['tx_index_in_block'],4)
        for name in ('ics-config','reserve-config'):
            self.assertEqual(json.loads((PROFILE/(name+'.json')).read_text())['asset']['asset_name'],'NIGHT')


if __name__=='__main__':unittest.main()
