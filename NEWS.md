# qsub 1.1.2

* BUG FIX: Copy paste the relevant code from `tools::R_user_dir()` into `config_file_location()` to ensure
  that qsub works with older versions of R.

# qsub 1.1.1

* BUG FIX: Surround `readRDS()` with `tryCatch()` such that if there is an rds object but it is
  truncated, qsub handles this as if there was no file at all.
  
* MINOR CHANGE: `config_file_location()` now uses `tools::R_user_dir()` to determine the path of the config file.

# qsub 1.1.0 (13-02-2019)

* MINOR CHANGE: There is now an option to compress the output files, which is turned
  on by default. 
  
* MINOR CHANGE: Allow unix systems to use `rsync` instead of `cp` for fetching the qsub output.

* BUG FIX: Fix `qsub_retrieve()` when processing the output; it did not take into account that
  `batch_tasks` could not be equal to 1.
  
* MINOR CHANGE: `qsub_retrieve()` now uses pbapply when loading in the output.

* BUG FIX: Test ssh connection pointer before using it.

* BUG FIX: Do not remove the first line of an error file.

* MINOR CHANGE: Use absolute paths to output standard output and error logs (#16, suggested by @mmehan).

* MINOR CHANGE; Allow rsync to also use the username@host:port notation (#16, suggested by @mmehan).

# qsub 1.0.0 (30-07-2018)

* INITIAL RELEASE: qsub allows you to run lapply() calls in parallel by submitting 
  them to gridengine clusters using the `qsub` command.
