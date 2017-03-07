#' Apply a Function over a List or Vector on a gridengine system!
#'
#' @param X A vector (atomic or list) or an expression object. Other objects (including classed objects) will be coerced by base::as.list.
#' @param FUN The function to be applied to each element of X.
#' @param qsub_config The configuration to use for this execution.
#' @param qsub_environment \code{NULL}, a character vector or an environment. Specifies what data and functions will be uploaded to the server.
#' @param ... optional arguments to FUN.
#'
#' @export
#'
#' @seealso \code{\link{create_qsub_config}}, \code{\link{set_default_qsub_config}}
#'
#' @examples
#' \dontrun{
#' # Initial configuration and execution
#' qsub_config <- create_qsub_config(
#'   remote = "myserver",
#'   local_tmp_path = "/home/myuser/workspace/.r2gridengine",
#'   remote_tmp_path = "/scratch/myuser/.r2gridengine"
#' )
#' qsub_lapply(
#'   X = seq_len(3),
#'   FUN = function(i) { Sys.sleep(1); i+1 },
#'   qsub_config = qsub_config
#' )
#'
#' # Setting a default configuration and short hand notation for execution
#' set_default_qsub_config(qsub_config, permanent = T)
#' qsub_lapply(seq_len(3), function(i) { Sys.sleep(1); i+1 })
#'
#' # Overriding a default qsub_config
#' qsub_lapply(seq_len(3), function(i) i + 1,
#'   qsub_config = override_qsub_config(name = "MyJob"))
#'
#' # Don't wait for results, get a handle instead and retrieve later.
#' handle <- qsub_lapply(seq_len(3), function(i) i + 1,
#'   qsub_config = override_qsub_config(wait = F))
#'
#' # Wait until results have been generated on the remote
#'
#' # Retrieve results
#' qsub_retrieve(handle)
#' }
qsub_lapply <- function(X, FUN, qsub_config = NULL, qsub_environment = NULL, ...) {
  dot_params <- list(...)

  # get default config
  if (is.null(qsub_config)) {
    qsub_config <- get_default_qsub_config()
  }
  test_qsub_config(qsub_config)

  # process environment names if necessary
  if (is.character(qsub_environment)) {
    environment_names <- qsub_environment
    qsub_environment <- NULL
  } else {
    environment_names <- NULL
  }

  # get the relevant environment
  if (is.null(qsub_environment)) {
    qsub_environment <- collect_environment_recursively(environment(FUN), environment_names)
  }

  if (!is.environment(qsub_environment)) {
    stop(sQuote("qsub_environment"), " must be NULL, a character vector, or an environment")
  }

  # determine seeds
  seeds <- sample.int(length(X)*10, length(X), replace = F)

  # collect arguments
  qsub_environment$PRISM_IN_THE_STREETS_OF_LONDON_PARAMS <- list(
    SEEDS = seeds,
    X = X,
    FUN = FUN,
    DOTPARAMS = dot_params
  )

  # generate folder names
  qsub_instance <- instantiate_qsub_config(qsub_config)

  # set number of tasks
  qsub_instance$num_tasks <- length(X)

  # upload all files to the remote
  setup_execution(qsub_instance, qsub_environment)

  # submit and retrieve job_id
  qsub_instance$job_id <- execute_job(qsub_instance)

  # retrieve output if so demanded
  if (qsub_instance$wait) {
    qsub_retrieve(qsub_instance)
  } else {
    qsub_instance
  }
}

collect_environment_recursively <- function(parent, environment_names) {
  child <- new.env()
  while(!is.null(parent)) {
    params <- names(parent)[!names(parent) %in% names(child)]
    if (!is.null(environment_names)) {
      params <- params[params %in% environment_names]
    }
    for (p in params) {
      assign(p, parent[[p]], child)
    }

    if (!identical(parent, globalenv())) {
      parent <- parent.env(parent)
    } else {
      parent <- NULL
    }
  }
  child
}

setup_execution <- function(qsub_config, qsub_environment) {
  with(qsub_config, {
    # check whether folders exist
    if (file_exists_remote(src_dir, remote = "", verbose = verbose)) {
      stop("The local temporary folder already exists!")
    }
    if (file_exists_remote(remote_dir, remote = remote, verbose = verbose)) {
      stop("The remote temporary folder already exists!")
    }

    # create folders
    mkdir_remote(src_dir, remote = "", verbose = verbose)
    mkdir_remote(remote_dir, remote = remote, verbose = verbose)

    # create log and output files
    mkdir_remote(src_outdir, remote = "", verbose = verbose)
    mkdir_remote(src_logdir, remote = "", verbose = verbose)

    # save environment
    save(list = names(qsub_environment), file = src_rdata, envir = qsub_environment)

    # write r script
    r_script <- paste0(
      "setwd(\"", remote_dir, "\")\n",
      "load(\"data.RData\")\n",
      "index <- as.integer(commandArgs(trailingOnly=T)[[1]])\n",
      "params <- PRISM_IN_THE_STREETS_OF_LONDON_PARAMS\n",
      "set.seed(params$SEEDS[[index]])\n",
      "out <- do.call(params$FUN, c(list(params$X[[index]]), params$DOTPARAMS))\n",
      "saveRDS(out, file=paste0(\"out/out_\", index, \".rds\", sep=\"\"))\n"
    )
    write_remote(r_script, src_rfile, remote = "", verbose = verbose)

    # write sh script
    sh_script <- with(qsub_config, paste0(
      "#!/bin/bash\n",
      ifelse(num_cores == 1, "", paste0("#$ -pe serial ", num_cores, "\n")),
      ifelse(is.numeric(max_running_tasks), paste0("#$ -tc ", max_running_tasks, "\n"), ""),
      "#$ -t 1-", num_tasks, "\n",
      "#$ -N ", name, "\n",
      "#$ -e log/log.$TASK_ID.e.txt\n",
      "#$ -o log/log.$TASK_ID.o.txt\n",
      ifelse(is.null(max_tasks) || !is.numeric(max_tasks) || !is.finite(max_tasks) || x != round(max_tasks), "", paste0("#$ -tc ", max_tasks, "\n")),
      "#$ -l h_vmem=", memory, "\n",
      "cd ", remote_dir, "\n",
      "module unload R\n",
      "module load ", r_module, "\n",
      paste0(paste0(execute_before, collapse="\n"), "\n"),
      "Rscript script.R $SGE_TASK_ID\n"
    ))
    write_remote(sh_script, src_shfile, remote = "", verbose = verbose)

    # rsync local with remote
    rsync_remote(
      remote_src = "",
      path_src = paste0(src_dir, "/"),
      remote_dest = remote,
      path_dest = paste0(remote_dir, "/")
    )

    NULL
  })
}

execute_job <- function(qsub_config) {
  list2env(qsub_config, environment())

  # start job remotely and read job_id
  submit_command <- paste0("cd ", remote_dir, "; qsub script.sh")
  output <- run_remote(submit_command, remote = remote, verbose = verbose)

  # retrieve job id
  job_id <- gsub(".*Your job-array ([0-9]*)[^\n]* has been submitted.*", "\\1", paste(output$cmd_out, collapse = "\n"))

  as.character(job_id)
}

#' Calculate the results of a function on PRISM!
#'
#' @param FUN the function to be executed.
#' @param qsub_config The configuration to use for this execution.
#' @param qsub_environment \code{NULL}, a character vector or an environment.
#' Specifies what data and functions will be uploaded to the server.
#' @param ... optional arguments to FUN.
#'
#' @seealso \code{\link{create_qsub_config}}, \code{\link{set_default_qsub_config}}
#'
#' @export
qsub_run <- function(FUN, qsub_config = NULL, qsub_environment = NULL, ...) {
  qsub_lapply(X = 1, FUN, qsub_config = qsub_config, qsub_environment = qsub_environment, ...)
}

#' Check whether a job is running.
#'
#' @param qsub_config The qsub configuration of class \code{qsub_configuration}, as returned by any qsub execution
#'
#' @export
is_job_running <- function(qsub_config) {
  list2env(qsub_config, environment())
  if (!is.null(job_id)) {
    qstat_out <- run_remote("qstat", remote)$cmd_out
    any(grepl(paste0("^ *", job_id, " "), qstat_out))
  } else {
    F
  }
}

#' Retrieve the results of a qsub execution.
#'
#' @param qsub_config The qsub configuration of class \code{qsub_configuration}, as returned by any qsub execution
#' @param wait If \code{TRUE}, wait until the execution has finished in order to return the results, else returns \code{NULL} if execution is not finished.
#' @importFrom readr read_file
#' @export
qsub_retrieve <- function(qsub_config, wait=T) {
  if (!wait && is_job_running(qsub_config)) {
    return(NULL)
  } else {
    list2env(qsub_config, environment())

    while (is_job_running(qsub_config)) {
      Sys.sleep(1)
    }

    # copy results to local
    rsync_remote(
      remote_src = remote,
      path_src = paste0(remote_dir, "/"),
      remote_dest = "",
      path_dest = paste0(src_dir, "/")
    )


    # read RData files
    tryCatch({
      outs <- lapply(seq_len(num_tasks), function(i) {
        out <- NULL # satisfying r check
        output_file <- paste0(src_dir, "/out/out_", i, ".rds")
        error_file <- paste0(src_dir, "/log/log.", i, ".e.txt")
        if (file.exists(output_file)) {
          out <- readRDS(output_file)
        } else {
          if (file.exists(error_file)) {
            msg <- sub("^[^\n]*\n", "", readr::read_file(error_file))
            txt <- paste0("File: ", error_file, "\n", msg)
          } else {
            txt <- paste0("File: ", error_file, "\nNo output or log file found. Did the job run on PRISM at all?")
          }
          if (stop_on_error) {
            stop(txt)
          } else {
            warning(txt)
            NA
          }
        }
      })
    }, error = function(e) {
      stop(e)
    }, finally = {
      # remove temporary folders afterwards
      if (remove_tmp_folder) {
        run_remote(paste0("rm -rf \"", remote_dir, "\""), remote = remote, verbose =verbose)
        run_remote(paste0("rm -rf \"", src_dir, "\""), remote = "", verbose = verbose)
      }
    })

    return(outs)
  }
}
