# qsub 1.1.1

* BUG FIX: Surround `readRDS()` with `tryCatch()` such that if there is an rds object but it is
  truncated, qsub handles this as if there was no file at all.
  
* MINOR CHANGE: `config_file_location()` now uses `tools::R_user_dir()` to determine the path of the config file.

## Test environments
* local Fedora install, R 4.0.3
* ubuntu 14.04 (on travis-ci)
* win-builder (devel and release)

## R CMD check results

Duration: 1m 33s

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

R CMD check succeeded
