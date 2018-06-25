#' Run qacct on remote
#'
#' @param qsub_config The config
#'
#' @export
qacct <- function(qsub_config) {
  if (!"job_id" %in% names(qsub_config)) {
    stop(sQuote("qsub_config"), " is not a qsub handle.")
  }
  job_id <- qsub_config$job_id
  if (is_job_running(qsub_config)) {
    warning("job ", job_id, " is still running.")
  }

  remote <- qsub_config$remote

  qacct_remote(remote, job_id)
}

#' Run qacct on remote
#'
#' @param remote The remote
#' @param job_id The job_id of the job
#'
#' @export
qacct_remote <- function(remote, job_id) {
  out <- run_remote(paste0("qacct -j ", job_id), remote)$stdout
  if (str_detect(out, "job id \\d* not found")[[1]]) {
    NULL
  } else {
    breaks <- c(which(str_detect(out, "======")), length(out)+1)
    map_df(seq_len(length(breaks)-1), function(i) {
      strs <- out[seq(breaks[[i]]+1, breaks[[i+1]]-1)]
      data_frame(
        job_id,
        row_number_i = i,
        name = gsub(" *$", "", stringr::str_sub(strs, 1, 12)),
        value = gsub(" *$", "", stringr::str_sub(strs, 14, -1))
      )
    }) %>%
      spread_("name", "value")
  }
}
