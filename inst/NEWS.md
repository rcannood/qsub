# qsub 1.1.0 (unreleased)

* MINOR CHANGE: There is now an option to compress the output files, which is turned
  on by default. 
  
* MINOR CHANGE: Allow unix systems to use `rsync` instead of `cp` for fetching the qsub output.

* BUG FIX: Let `post_fun` take into account the `batch_tasks` parameter; output files could consist of 
  multiple tasks.

# qsub 1.0.0 (30-07-2018)

* INITIAL RELEASE: qsub allows you to run lapply() calls in parallel by submitting 
  them to gridengine clusters using the `qsub` command.