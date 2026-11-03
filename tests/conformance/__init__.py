"""Node-query conformance harness and fail-closed drills.

Runs the exact SQL text of every query midnight-node issues, with the parameter types, binary
encodings and protocol flow of its SQLx driver, against a node-facing endpoint, and compares the
answers with a frozen corpus of cardano-db-sync answers. Started through `tools/dev.py conformance`
and `tools/dev.py drills`; neither is part of the normal lanes.
"""
