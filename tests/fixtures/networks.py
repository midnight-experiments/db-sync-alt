"""The packaged network profiles and the Preprod acquisition profile the tests use."""
from db_sync_alt.contract import Profile
from db_sync_alt.networks import network_profile
from db_sync_alt.resources import profile_dir

PREPROD = network_profile('preprod')
PREPROD_PROFILE = Profile(profile_dir('preprod'))
