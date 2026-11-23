#!/usr/bin/env python3
"""Check the complete public tree; report filenames/codes, never secret matches."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import subprocess

ROOT_FILES = {'README.md','LICENSE','NOTICE','THIRD_PARTY_NOTICES.md','THIRD_PARTY_LICENSES.txt','CONTRIBUTING.md',
              'SECURITY.md','CHANGELOG.md','CODE_OF_CONDUCT.md','.gitignore','.dockerignore',
              '.env.example','Cargo.toml','Cargo.lock','rust-toolchain.toml'}
ROOT_DIRS = {'profiles','reference','deploy','tools','docs','.github','crates'}
PATTERNS = {
    'personal_absolute_path': re.compile(r'/(?:home|Users|media)/[A-Za-z0-9_.-]+/'),
    'private_key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----'),
    'provider_credential': re.compile(r'\b(?:mainnet|preprod|preview)[A-Za-z0-9]{32}\b'),
    'github_credential': re.compile(r'\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b'),
    'credential_url': re.compile(r'(?:postgres(?:ql)?|https?)://[^\s/:]+:[^\s/@]+@'),
    'private_host_identity': re.compile(r'\b(?:eddie|acedward)@(?:[^\s]+)'),
    # References to the development process: a plan's test numbers as a
    # comment prefix, question numbers, requirement numbers, and short commit
    # hashes named as history. Text states what the software is now.
    'plan_test_number': re.compile(r'//[/!]?[ \t]*Tests? \d+\b'),
    'question_reference': re.compile(r'(?<![A-Za-z0-9])[Qq]\d+(?![A-Za-z0-9])'),
    'requirement_reference': re.compile(r'\b(?:FR|SC)-\d+\b'),
    'commit_as_history': re.compile(
        r'(?i:\b(?:commit|from|at|since|before|after|exact|in|of)[ \t]+`?)'
        r'(?=[0-9a-f]{0,6}[a-f])(?=[0-9a-f]{0,6}[0-9])[0-9a-f]{7}(?![0-9A-Za-z_])'),
}
# Binary assets require an exact path and SHA-256. Compressed JSON fixtures
# additionally receive bounded expansion, member validation and text scans.
APPROVED_PNG = {}
PNG_SIGNATURE = bytes((137, 80, 78, 71, 13, 10, 26, 10))
APPROVED_JSON_GZIP = {
    'crates/dsa-provider/tests/fixtures/mainnet-corpus.json.gz':
        '0195eee4b707ace0f3095902009ff66350288e5f7bf732b6450912377ead5f4f',
    'crates/dsa-provider/tests/fixtures/preprod-corpus.json.gz':
        '7111c2e00ccd86ec320a3e5fe36f49c581144e13de8c8acfc3eac7a970cfec8c',
}

FORBIDDEN_SUFFIXES = {'.dump','.db','.pem','.key','.log','.pyc','.pyo'}

# Exact synthetic credential strings (redaction tests) and documentation placeholders only;
# never allow an entire file or pattern.
NODE_CONNECTION_EXAMPLE = 'postgresql://db_sync_alt_reader:<password>@<host>:5432/db_sync_alt'
REDACTION_FIXTURES = {
    'README.md': {NODE_CONNECTION_EXAMPLE},
    'docs/connect-a-node.md': {NODE_CONNECTION_EXAMPLE},
    # Upstream unit-test constant in the pinned (hash-checked) node source.
    'reference/node/src/main_chain_follower.rs': {'postgres://user:pass@localhost:5432/testdb'},
}

def recorded_fixture_text(raw):
    if len(raw)>5*1024*1024:raise ValueError('compressed_fixture_limit')
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
        expanded=stream.read(64*1024*1024+1)
    if len(expanded)>64*1024*1024:raise ValueError('expanded_fixture_limit')
    bundle=json.loads(expanded)
    if bundle['format']!='recorded-corpus-v1' or bundle['network'] not in {'mainnet','preprod'}:
        raise ValueError('fixture_identity')
    files=bundle['files'];hashes=bundle['sha256']
    if not isinstance(files,dict) or not files or files.keys()!=hashes.keys():
        raise ValueError('fixture_members')
    for name,text in files.items():
        parts=Path(name).parts
        allowed=name in {'recording-plan.json','expected.json','origins.json'} or (
            len(parts)==3 and parts[0]=='provider' and
            parts[1] in {'headers','transactions','datums','metadata','outputs'} and
            parts[2].endswith('.json'))
        if not allowed or name!='/'.join(parts) or Path(name).is_absolute() or any(p in {'.','..'} for p in parts):
            raise ValueError('fixture_member_path')
        if not isinstance(text,str) or hashlib.sha256(text.encode()).hexdigest()!=hashes[name]:
            raise ValueError('fixture_member_hash')
        json.loads(text)
    return '\n'.join(files.values())

# Codes in the first column of these tables must exist as string literals in
# the crates' sources, so that the page names no code the software lacks.
TROUBLESHOOTING='docs/troubleshooting.md'

def heading_anchors(text):
    """GitHub's anchors of a Markdown file's headings, outside code fences."""
    anchors=set();seen={};fenced=False
    for line in text.split('\n'):
        if line.lstrip().startswith('```'):fenced=not fenced;continue
        match=None if fenced else re.match(r'(#{1,6})\s+(.*?)\s*$',line)
        if not match:continue
        slug=re.sub(r'[^\w\- ]','',match.group(2).lower()).replace(' ','-')
        count=seen.get(slug,0);seen[slug]=count+1
        anchors.add(slug if count==0 else f'{slug}-{count}')
    return anchors

def source_literals(root):
    found=set()
    for path in sorted((root/'crates').glob('*/src/**/*.rs')):
        found.update(re.findall(r'"([a-z][a-z0-9_]*)"',path.read_text(errors='replace')))
    return found

def troubleshooting_errors(root):
    page=root/TROUBLESHOOTING
    if not page.exists():return []
    known=source_literals(root)
    codes=re.findall(r'^\| `([a-z][a-z0-9_]*)` \|',page.read_text(),re.MULTILINE)
    if not codes:return [(TROUBLESHOOTING,'no_codes')]
    return [(TROUBLESHOOTING,'unknown_code:'+code) for code in codes if code not in known]

# The prose of the repository describes the software as it is, from its first
# version on: no earlier db-sync-alt version, upgrade or comparison with before.
# Pinned upstream files under reference/ and generated notices are not prose.
PROSE_FILES = {'README.md','CHANGELOG.md','SECURITY.md','CONTRIBUTING.md','CODE_OF_CONDUCT.md','NOTICE',
               '.github/pull_request_template.md'}
PROSE_DIRS = ('docs/','deploy/','.github/ISSUE_TEMPLATE/')
HISTORY = {
    # 0.1.x to 0.4.x, also as v0.4.9; not part of a longer number or address.
    'earlier_version': re.compile(r'(?<![\w.])v?0\.[0-4]\.(?:\d+|x)(?![\w]|\.\d)'),
    'upgrade': re.compile(r'\bupgrad(?:e|es|ed|ing)\b',re.IGNORECASE),
    'migration': re.compile(r'\bmigrat(?:e|es|ed|ing|ion|ions)\b',re.IGNORECASE),
    'earlier_release': re.compile(r'\b(?:earlier|previous|prior|older|former)\s+(?:db-sync-alt\s+)?'
                                  r'(?:release|version|implementation|format)s?\b',re.IGNORECASE),
    'python_implementation': re.compile(r'\bPython\s+(?:implementation|package|release|version)s?\b',re.IGNORECASE),
    'breaking_release': re.compile(r'\*\*Breaking\b|\bbreaking\s+release\b',re.IGNORECASE),
    'compared_with_before': re.compile(r'\bno longer\b|\b(?:is|are)\s+gone\b',re.IGNORECASE),
}

def is_prose(rel):
    name=rel.as_posix()
    if name.startswith('reference/'):return False
    if name in PROSE_FILES or rel.name=='README.md':return True
    return name.startswith(PROSE_DIRS) and (not name.startswith('docs/') or rel.suffix=='.md')

def history_errors(rel,text):
    found=[]
    for number,line in enumerate(text.split('\n'),1):
        for label,regex in HISTORY.items():
            if regex.search(line):found.append((str(rel),f'{label}:line {number}'))
    return found

# The images CI runs in are the ones the image build and Compose use, so that
# an update proposed for deploy/ fails CI until the workflow follows.
CI_WORKFLOW='.github/workflows/ci.yml'

def workflow_image_errors(root):
    files=[root/CI_WORKFLOW,root/'deploy/Dockerfile',root/'deploy/compose.yaml']
    if not all(path.exists() for path in files):return []
    ci,dockerfile,compose=(path.read_text() for path in files)
    pairs={
        'rust':(re.findall(r'^\s*container:\s*(rust:\S+)\s*$',ci,re.M),
                re.findall(r'^FROM\s+(rust:\S+)',dockerfile,re.M)),
        'postgres':(re.findall(r'^\s*image:\s*(postgres:\S+)\s*$',ci,re.M),
                    re.findall(r'^\s*image:\s*(postgres:\S+)\s*$',compose,re.M)),
    }
    return [(CI_WORKFLOW,'workflow_image_differs:'+name) for name,(used,pinned) in pairs.items()
            if not used or not pinned or set(used)!=set(pinned) or '@sha256:' not in used[0]]

def check(root):
    errors=[]; hashes={}
    tracked=set()
    if (root/'.git').exists():
        tracked=set(subprocess.check_output(['git','-C',str(root),'ls-files','-z']).decode().split('\0'))
    for p in sorted(root.rglob('*')):
        rel=p.relative_to(root)
        if rel.parts[0]=='.git':continue
        # Only untracked Python caches are omitted in a working checkout; exports are strict.
        if (root/'.git').exists() and '__pycache__' in rel.parts and str(rel) not in tracked:continue
        if p.is_symlink():errors.append((str(rel),'symlink'));continue
        if p.is_dir():continue
        if rel.parts[0] not in ROOT_DIRS and str(rel) not in ROOT_FILES:
            errors.append((str(rel),'unexpected_distribution_file'))
        if p.suffix.lower() in FORBIDDEN_SUFFIXES or p.name in {'.env','pgpass','compose.env'}:
            errors.append((str(rel),'operational_artifact'))
        b=p.read_bytes();hashes[str(rel)]=hashlib.sha256(b).hexdigest()
        if str(rel) in APPROVED_PNG:
            if hashes[str(rel)]!=APPROVED_PNG[str(rel)] or not b.startswith(PNG_SIGNATURE):
                errors.append((str(rel),'unapproved_binary_asset'))
            continue
        if str(rel) in APPROVED_JSON_GZIP:
            if hashes[str(rel)]!=APPROVED_JSON_GZIP[str(rel)]:
                errors.append((str(rel),'unapproved_binary_asset'));continue
            try:text=recorded_fixture_text(b)
            except (OSError,EOFError,ValueError,TypeError,KeyError,AttributeError):
                errors.append((str(rel),'invalid_recorded_fixture'));continue
        else:
            try:text=b.decode('utf-8')
            except UnicodeDecodeError:errors.append((str(rel),'binary_file'));continue
        if '\x00' in text:errors.append((str(rel),'binary_file'));continue
        for label,regex in PATTERNS.items():
            scan_text=text
            if str(rel)=='.github/scripts/check_public_source.py' and label=='credential_url':
                for values in REDACTION_FIXTURES.values():
                    for literal in values:scan_text=scan_text.replace(literal,'REDACTION_FIXTURE')
            matches=list(regex.finditer(scan_text))
            if label=='credential_url':
                # URL regex ends at @; bind the exception to the full literal fixture.
                allowed=REDACTION_FIXTURES.get(str(rel),set())
                matches=[m for m in matches if not any(text[m.start():].startswith(v) and text[m.start()+len(v):m.start()+len(v)+1] in {chr(39),chr(34)} for v in allowed)]
            if matches:errors.append((str(rel),label))
        if is_prose(rel):errors+=history_errors(rel,text)
        if p.suffix=='.md':
            for link in re.findall(r'\[[^\]]*\]\(([^)]+)\)',text):
                target,_,fragment=link.partition('#')
                if re.match(r'[a-zA-Z][a-zA-Z0-9+.-]*:',target):continue
                destination=(p.parent/target).resolve() if target else p.resolve()
                if not destination.is_relative_to(root):errors.append((str(rel),'external_local_link'))
                elif not destination.exists():errors.append((str(rel),'broken_link:'+target))
                elif fragment and destination.suffix=='.md' and fragment not in heading_anchors(
                        destination.read_text(errors='replace')):
                    errors.append((str(rel),'broken_anchor:'+link))
    errors+=troubleshooting_errors(root)
    errors+=workflow_image_errors(root)
    try:
        profiles=sorted(p.parent for p in (root/'profiles').glob('*/source-manifest.json'))
        if not profiles:raise ValueError('no_profiles')
        for folder in (root/'reference',*profiles):
            manifest=json.loads((folder/'source-manifest.json').read_text())
            for name,entry in manifest['files'].items():
                rel=str((folder/name).relative_to(root))
                if hashes.get(rel)!=entry['sha256']:errors.append((rel,'pinned_hash_mismatch'))
    except (OSError,ValueError,KeyError):errors.append(('provenance','missing_or_invalid_manifest'))
    return errors,hashes

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--manifest',type=Path,help='Write content hashes outside the public tree')
    args=parser.parse_args();root=args.root.resolve()
    if args.manifest and args.manifest.resolve().is_relative_to(root):parser.error('manifest must be outside the scanned tree')
    errors,hashes=check(root)
    for name,code in errors:print(name+': '+code)
    if errors:print('Public-source check failed: '+str(len(errors))+' finding(s).');return 1
    if args.manifest:args.manifest.write_text(json.dumps(hashes,sort_keys=True,indent=2)+'\n')
    print('Public-source check passed: '+str(len(hashes))+' files; pinned references and profiles unchanged.')
    return 0
if __name__=='__main__':sys.exit(main())
