# SPDX-License-Identifier: Apache-2.0
"""The premise behind `registered_candidates_unserved`, pinned against the packaged node reference.

db-sync-alt serves no registered candidates: the registered-candidates address is not a profile address.
That is safe for consensus only while the node's committee selection passes an empty registered-candidates
list and never queries registrations. A node bump whose reference restores the query fails here.
"""
import json
import re
import unittest

from db_sync_alt.resources import data_dir

SOURCE = 'partner-chains/toolkit/committee-selection/authority-selection-inherents/src/authority_selection_inputs.rs'
PREMISE = ('registered_candidates_unserved premise broken: committee selection '
           '(AuthoritySelectionInputs::from_mc_data) must pass an empty registered-candidates list and must not '
           'query registrations; db-sync-alt serves no registered candidates, so this node would select a different '
           'committee from it')
EMPTY = re.compile(r'let\s+registered_candidates\s*:\s*Vec<CandidateRegistrations>\s*=\s*Vec::new\(\)\s*;')
# The query as node 1.0.400 ran it, in place of the empty list.
RESTORED = '''		let registered_candidates: Vec<CandidateRegistrations> = candidate_data_source
			.get_candidates(for_epoch, scripts.committee_candidate_address.clone())
			.await
			.map_err(|err| {
				AuthoritySelectionInputsCreationError::GetCandidatesQuery(
					for_epoch,
					scripts.committee_candidate_address.to_string(),
					err,
				)
			})?;'''


def from_mc_data(text):
    """The body of `from_mc_data`: from the first brace after its signature to the matching one."""
    start = text.find('fn from_mc_data(')
    if start < 0:
        return None
    opened = text.index('{', start)
    depth = 0
    for index in range(opened, len(text)):
        depth += {'{': 1, '}': -1}.get(text[index], 0)
        if depth == 0:
            return text[opened:index + 1]
    return None


def violations(text):
    """What breaks the premise in one version of the committee-selection source; empty when it holds."""
    body = from_mc_data(text)
    if body is None:
        return ['from_mc_data not found']
    out = []
    # The data source trait declares get_candidates for the RPCs; only a call in from_mc_data selects with it.
    if 'get_candidates' in body:
        out.append('get_candidates is called')
    if len(EMPTY.findall(body)) != 1:
        out.append('registered_candidates is not Vec::new()')
    if not re.search(r'Ok\(Self\s*\{[^}]*\bregistered_candidates\b[^}]*\}\)', body):
        out.append('the inputs are not built from that list')
    return out


class CommitteeSelectionPremiseTests(unittest.TestCase):
    def check(self, text):
        self.assertEqual(violations(text), [], PREMISE)

    def test_the_pinned_node_selects_committees_without_registrations(self):
        reference = data_dir('reference')
        self.assertIn(SOURCE, json.loads((reference / 'source-manifest.json').read_text())['files'])
        self.check((reference / SOURCE).read_text())

    def test_a_reference_that_restores_the_query_fails_with_the_premise(self):
        text = (data_dir('reference') / SOURCE).read_text()
        self.assertEqual(len(EMPTY.findall(text)), 1)
        restored = EMPTY.sub(lambda _: RESTORED.strip(), text)
        self.assertEqual(violations(restored), ['get_candidates is called', 'registered_candidates is not Vec::new()'])
        with self.assertRaisesRegex(AssertionError, 'registered_candidates_unserved premise broken'):
            self.check(restored)
        renamed = text.replace('fn from_mc_data(', 'fn from_main_chain_data(')
        with self.assertRaisesRegex(AssertionError, 'registered_candidates_unserved premise broken'):
            self.check(renamed)


if __name__ == '__main__':
    unittest.main()
