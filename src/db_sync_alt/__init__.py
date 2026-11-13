"""db-sync-alt: Cardano data for Midnight's main-chain follower, kept in PostgreSQL."""
import sys

__version__ = '0.4.7'

# A Plutus integer is unbounded: one datum integer can fill a transaction (16 KiB is about 39,500
# decimal digits), and cardano-db-sync stores it exactly. Python converts at most 4,300 digits
# between int and text by default; this keeps such integers exact in JSON.
INTEGER_DIGITS = 100_000
if 0 < sys.get_int_max_str_digits() < INTEGER_DIGITS:
    sys.set_int_max_str_digits(INTEGER_DIGITS)
