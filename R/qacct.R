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
#' @importFrom stringr str_sub
#' @export
qacct_remote <- function(remote, job_id) {
  out <- run_remote(paste0("qacct -j ", job_id), remote)$cmd_out
  if (grepl("job id \\d* not found", out)[[1]]) {
    NULL
  } else {
    breaks <- c(which(grepl("======", out)), length(out)+1)
    bind_rows(lapply(seq_len(length(breaks)-1), function(i) {
      strs <- out[seq(breaks[[i]]+1, breaks[[i+1]]-1)]
      names <- gsub(" *$", "", stringr::str_sub(strs, 1, 12))
      values <- gsub(" *$", "", stringr::str_sub(strs, 14, -1))
      data_frame(job_id, task_id = i, name = names, value = values)
    })) %>% spread(name, value)
  }
}

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
#' @importFrom stringr str_sub
#' @export
qstat_j_remote <- function(remote, job_id) {
  out <- run_remote(paste0("qstat -j ", job_id), remote)$cmd_out

  if (grepl("Following jobs do not exist:", out)[[1]]) {
    NULL
  } else {
    breaks <- c(which(grepl("======", out)), length(out)+1)
    bind_rows(lapply(seq_len(length(breaks)-1), function(i) {
      strs <- out[seq(breaks[[i]]+1, breaks[[i+1]]-1)]
      names <- gsub(": *$", "", stringr::str_sub(strs, 1, 27))
      values <- gsub(" *$", "", stringr::str_sub(strs, 29, -1))
      data_frame(job_id, task_id = i, name = names, value = values)
    })) %>% spread(name, value)
  }
}
