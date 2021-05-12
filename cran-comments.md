# qsub 1.1.2

* BUG FIX: Copy paste the relevant code from `tools::R_user_dir()` into `config_file_location()` to ensure
  that qsub works with older versions of R.
  
* TESTING: Skip test on CRAN which might result in the creation of the cache dir as specified by R_user_dir 
  if it did not already exist.

## Test environments
* local Fedora install, R 4.0.3
* ubuntu 14.04 (on travis-ci)
* win-builder (devel and release)

## R CMD check results

```
── R CMD check results ───────────────────────────────────────── qsub 1.1.2 ────
Duration: 31.7s

0 errors ✓ | 0 warnings ✓ | 0 notes ✓

R CMD check succeeded
```
