"""Locate the pinned data shipped with the package: profiles and reference sources."""
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent


def data_dir(name):
    """`profiles` or `reference`: inside the installed package, or at the root of a source checkout."""
    for candidate in (PACKAGE / name, PACKAGE.parents[1] / name):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(name)


def profile_dir(network):
    """Directory of the packaged acquisition profile of `network` (`mainnet`, `preprod`, `preview`)."""
    from db_sync_alt.networks import directory
    return directory(network)


def service_template():
    """Path of the service configuration template used by `run` and `compose init`."""
    return PACKAGE / 'service' / 'service.example.json'
