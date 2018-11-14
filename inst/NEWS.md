# qsub 1.1.0 (unreleased)

* MINOR CHANGE: There is now an option to compress the output files, which is turned
  on by default. 
  
* MINOR CHANGE: Allow unix systems to use `rsync` instead of `cp` for fetching the qsub output.

* BUG FIX: Fix `qsub_retrieve()` when processing the output; it did not take into account that
  `batch_tasks` could not be equal to 1.
  
* MINOR CHANGE: `qsub_retrieve()` now uses pbapply when loading in the output.

* BUG FIX: Test ssh connection pointer before using it.

# qsub 1.0.0 (30-07-2018)

* INITIAL RELEASE: qsub allows you to run lapply() calls in parallel by submitting 
  them to gridengine clusters using the `qsub` command.
