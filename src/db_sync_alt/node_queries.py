# SPDX-License-Identifier: Apache-2.0
"""Exact Midnight node query catalogue, read from the pinned reference sources."""
import re

from db_sync_alt.resources import data_dir

REFERENCE=data_dir('reference')

def norm(sql):return re.sub(r'\s+',' ',sql).strip().rstrip(';').lower()
def catalogue():
 out={}
 for f in sorted(REFERENCE.rglob('*.rs')):
  s=f.read_text()
  for m in re.finditer(r'r#"([\s\S]*?)"#|(?<!#)"([^"\\]*(?:\\.[^"\\]*)*)"',s):
   text=m.group(1) or m.group(2)
   if not text.strip().upper().startswith(('SELECT','CREATE INDEX','WITH ')):continue
   if '{' in text or '\\' in text:continue
   fn=re.findall(r'fn\s+(\w+)\s*\(',s[:m.start()]);name=fn[-1] if fn else 'unknown'
   key=norm(text);source={'name':name,'file':str(f.relative_to(REFERENCE))}
   if key in out:out[key]['sources'].append(source)
   else:out[key]=dict(source,sql=text.strip().rstrip(';'),sources=[source])
 # SQLx driver setup/introspection (fixed query text; table data cannot be smuggled).
 extra=["SELECT $1::regtype::oid","SELECT oid FROM pg_catalog.pg_type WHERE typname = $1", "SELECT typname, typtype, typelem, typrelid, typbasetype FROM pg_catalog.pg_type WHERE oid = $1", "SET extra_float_digits = 3; SET application_name = 'sqlx'", "SET extra_float_digits = 3", "SELECT 1"]
 for s in extra:out[norm(s)]={'name':'driver','file':'sqlx fixed driver setup','sql':s}
 return out
CAT=catalogue()
def named(name,part=None):
 values=[v['sql'] for v in CAT.values() if any(s['name']==name and (not part or part in s['file']) for s in v.get('sources',[v]))]
 if len(values)!=1:raise ValueError((name,len(values)))
 return values[0]

