#!/usr/bin/env python3
"""Verify every vendored node file against the pinned upstream checkout."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--node-source',type=Path,required=True)
args=parser.parse_args()
reference=Path(__file__).resolve().parents[2]/'reference'
manifest=json.loads((reference/'source-manifest.json').read_text())
head=subprocess.check_output(['git','-C',str(args.node_source),'rev-parse','HEAD'],text=True).strip()
if head!=manifest['commit']:raise SystemExit('node_source_commit_mismatch')
for name,entry in manifest['files'].items():
    local=(reference/name).read_bytes()
    if hashlib.sha256(local).hexdigest()!=entry['sha256']:raise SystemExit('reference_hash_mismatch: '+name)
    if local!=(args.node_source/entry['source']).read_bytes():raise SystemExit('node_source_bytes_mismatch: '+name)
print('Pinned reference source verified:',len(manifest['files']),'files')
