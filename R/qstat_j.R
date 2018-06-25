#' Run qstat on remote
#'
#' @param qsub_config The config
#'
#' @export
qstat_j <- function(qsub_config) {
  if (!"job_id" %in% names(qsub_config)) {
    stop(sQuote("qsub_config"), " is not a qsub handle.")
  }
  job_id <- qsub_config$job_id
  if (!is_job_running(qsub_config)) {
    warning("job ", job_id, " is not running.")
  }

  remote <- qsub_config$remote

  qstat_j_remote(remote, job_id)
}

#' Run qstat on remote
#'
#' @param remote The remote
#' @param job_id The job_id of the job
#'
#' @export
qstat_j_remote <- function(remote, job_id) {
  out <- run_remote(paste0("qstat -j ", job_id), remote)$stdout

  if (str_detect(out, "Following jobs do not exist:")[[1]]) {
    NULL
  } else {
    name <- NULL ## appease r cmd check

    breaks <- c(which(str_detect(out, "======")), length(out)+1)
    map_df(seq_len(length(breaks)-1), function(i) {
      strs <- out[seq(breaks[[i]]+1, breaks[[i+1]]-1)]
      data_frame(
        job_id,
        task_id = i,
        name = gsub(": *$", "", stringr::str_sub(strs, 1, 27)),
        value = gsub(" *$", "", stringr::str_sub(strs, 29, -1))
      ) %>% filter(!str_detect(name, "^ *$"))
    }) %>%
      spread_("name", "value")
  }
}
