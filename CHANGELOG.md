All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.5] - 2023-12-04
### Fixed
- Fixed tasks freezing if there was no message in the exception
### Added
- Added `docker_dev` target in Makefile

## [1.3.4] - 2023-08-03
### Fixed
- Fixed `Otljob not found` error in ot_simple_rest_job_proxy
### Added
- Plugin version logging at startup

## [1.3.3] - 2023-02-16
### Fixed
- Add task that  removes forever calculating results

## [1.3.2] - 2023-02-06
### Fixed
- Fixed integrity error when two equal otl jobs created simultaneously 

## [1.3.1] - 2023-01-31
### Changed
- When node job failed put only node job uuid in status text instead of entire body
- Computing node activated when getting resource info 
- Set default encoding to utf-8 in ot_simple_rest_job_proxy

### Fixed
- Fixed error when nodejob canceled and awated results  nodejobs exists
- Delete otl job info from redis when otl job deleted


## [1.3.0] - 2022-12-02
### Added
- Add user guid in node job dictionary
- Add status filters for node job and otl job in django admin

## [1.2.2] - 2022-11-23
### Fixed
- Fixed error with complex_rest authentication in ot_simple_rest_job_proxy

## [1.2.1] - 2022-11-22
### Changed 
- Download miniconda 4.12 in local directory while building

## [1.2.0] - 2022-11-22
### Changed
- Using otlang version 1.2.1
### Added
- Add set_cache command.
- Add node job uuid in admin list display 
- Add complex rest authentication support to ot_simple_rest_job_proxy
### Fixed
- Fix otl job finished status before executing root job
- Fix ot_simple_rest_job_proxy views. Using ordinary django views.

## [1.1.0] - 2022-09-05
### Changed
- Job proxy checkjob view accepts POST requests
- Adapt otl interpreter to 1.2.0 otlang version
### Added
- Running tests in docker

## [1.0.5] - 2022-06-20
### Fixed
- Fix `__timestamp__` condition
- Fix mistyping in checking result status

## [1.0.4] - 2022-05-30
### Changed
- `use_timewindow` flag stored in NodeCommand model now
### Added
- Add `idempotent` flag to command model. If `False` then `__timestamp__` argument will be added in parsed command.

## [1.0.3] - 2022-05-23
### Fixed
- Planning node job for inactive computing nodes was fixed
- Fixed empty computing node pool when dispatcher restarted
### Added
- Canceling otl job by timeout
- Accepting url encoded data in simple rest proxy plugin
### Changed
- Job planner always creates node job for async subsearch 

## [1.0.2] - 2022-04-27
### Fixed
- Allow node job status transition from READY_TO_EXECUTE to WAITING_SAME_RESULT

## [1.0.1] - 2022-04-26
### Fixed
- OTLang too precise version

## [1.0.0] - 2022-04-25
### Added
- Proxy plugin for ot_simple_rest
- Result dataframe managing 
- Cancel job endpoint
### Fixed
- Mock computing node accepts canceled status
- Fix disappearing nodes from computing node pool when dispatcher restarts
- Change job status to FAILED when syntaz error
### Changed
- Otlang version to 1.1.0

## [0.1.0] - 2022-03-29
### Added
- Changelog.md.

### Changed
- Start using "changelog".
