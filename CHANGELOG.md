# Changelog

## v0.1.0
- switched to new ``hsm`` package and recall/HSM proxy for starting and organizing recalls
- considerably simplified code basis in the context of switching to the HSM proxy
- default delay for the queue to collect resources is set from 2 to 5 seconds

## v0.0.5
- a few new debugging logging messages
- improved way to set logging level for ``slkspec``

## v0.0.4
- incremented dependency on ``pyslk`` to ``2.3.0`` => improved workflows + new required functions
- internal ``list`` of files needed to be retrieved was changed to type ``set`` => remove duplicate files
- consider case when files are copied to the cache by another process while they are queued for being recalled from tape to cache by this instance of slkspec
- split recall and retrieval code into more sub-functions for clearer code structure

## v0.0.3
- adapted to ``pyslk`` version >= 2.0.0
- improved and parallelized retrieval workflow
- dependency on ``pyslk`` available via PyPI (and not DKRZ GitLab Repo)

## v0.0.2
- Add CHANGELOG [#50](https://github.com/observingClouds/slkspec/pull/50)
- Modernize packaging with pyproject.toml [#49](https://github.com/observingClouds/slkspec/pull/49)
- Drop python 3.7 and 3.8 support
- many more changes as documented in the [commit history](https://github.com/observingClouds/slkspec/compare/v0.0.1...v0.0.2)

## v0.0.1
- Initial showcase version
