#!/usr/bin/env bash
set -euo pipefail
private_dir="$HOME/.local/share/db-sync-alt/private"
if [[ -e "$private_dir" || -L "$private_dir" ]]; then
  printf '%s\n' 'Private configuration already exists; use the operations/recovery guide.' >&2
  exit 1
fi
IFS= read -r -p 'Accepted backup archive absolute path: ' archive </dev/tty
IFS= read -r -p 'Accepted manifest absolute path: ' manifest </dev/tty
IFS= read -r -p "Private Blockfrost key-file absolute path for the backup's network (not the key): " key_file </dev/tty
for input in "$archive" "$manifest" "$key_file"; do
  [[ "$input" == /* && -f "$input" && -r "$input" ]] || { printf '%s\n' 'Each input must be an absolute path to a readable file.' >&2; exit 1; }
done
PYTHONPATH="$(cd "$(dirname "$0")/.." && pwd)/src" python3 -m db_sync_alt compose init \
  --private "$private_dir" --archive "$archive" --manifest "$manifest" \
  --http-port 18080
install -m 600 "$key_file" "$private_dir/service/provider.key"
