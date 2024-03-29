#' Apply a Function over a List or Vector on a gridengine system!
#'
#' @param X A vector (atomic or list) or an expression object. Other objects (including classed objects) will be coerced by base::as.list.
#' @param FUN The function to be applied to each element of X.
#' @param object_envir The environment in which to go looking for the qsub_environment variables, if these are characters.
#' @param qsub_config The configuration to use for this execution.
#' @param qsub_environment \code{NULL}, a character vector or an environment. Specifies what data and functions will be uploaded to the server.
#' @param qsub_packages The packages to be loaded on the cluster.
#' @param ... optional arguments to FUN.
#'
#' @export
#'
#' @seealso \code{\link{create_qsub_config}}, \code{\link{set_default_qsub_config}}
#'
#' @importFrom ssh ssh_connect ssh_disconnect
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
qsub_lapply <- function(
  X,
  FUN,
  object_envir = environment(FUN),
  qsub_config = NULL,
  qsub_environment = NULL,
  qsub_packages = NULL,
  ...
) {
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
    qsub_environment <- collect_environment_recursively(object_envir, environment_names)
  }

  # check qsub_environment
  if (!is.environment(qsub_environment)) {
    stop(sQuote("qsub_environment"), " must be NULL, a character vector, or an environment")
  }

  # determine batches
  QSUB_START <- seq(1, length(X), by = qsub_config$batch_tasks)
  QSUB_STOP <- pmin(QSUB_START + qsub_config$batch_tasks - 1, length(X))

  # check compress param
  if (is.null(qsub_config$compress)) {
    qsub_config$compress <- "gz"
  }
  if (length(qsub_config$compress) > 1) {
    qsub_config$compress <- qsub_config$compress[[1]]
  }

  # determine seeds
  seeds <- sample.int(length(QSUB_START)*10, length(QSUB_START), replace = F)

  # collect arguments
  compress_map <- list(gz = "gzip", bz2 = "bzip2", xz = "xz", none = FALSE)
  prism_environment <- list(
    SEEDS = seeds,
    X = X,
    FUN = FUN,
    DOTPARAMS = dot_params,
    PACKAGES = qsub_packages,
    QSUB_START = QSUB_START,
    QSUB_STOP = QSUB_STOP,
    COMPRESS = compress_map[[qsub_config$compress]]
  )

  # generate folder names
  qsub_instance <- instantiate_qsub_config(qsub_config)

  # commence SSH connection
  if (!is_valid_ssh_connection(qsub_instance$remote_ssh)) {
    remote_ssh <- create_ssh_connection(qsub_config$remote)
    qsub_instance$remote_ssh <- remote_ssh
    on.exit(ssh::ssh_disconnect(remote_ssh))
  }

  # set number of tasks
  qsub_instance$num_tasks <- length(QSUB_START)

  # upload all files to the remote
  setup_execution(
    qsub_config = qsub_instance,
    qsub_environment = qsub_environment,
    prism_environment = prism_environment
  )

  # submit and retrieve job_id
  qsub_instance$job_id <- execute_job(qsub_instance)

  # retrieve output if so demanded
  if (qsub_instance$wait) {
    qsub_retrieve(qsub_instance)
  } else {
    qsub_instance$remote_ssh <- NULL
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

#' @importFrom readr write_lines write_rds
setup_execution <- function(
  qsub_config,
  qsub_environment,
  prism_environment
) {
  # short hand notation
  qs <- qsub_config
  remote <- qs$remote_ssh # this should have been created by qsub_lapply

  # check whether folders exist
  if (file_exists_remote(qs$src_dir, remote = FALSE, verbose = qs$verbose)) {
    stop("The local temporary folder already exists!")
  }

  if (file_exists_remote(qs$remote_dir, remote = remote, verbose = qs$verbose)) {
    stop("The remote temporary folder already exists!")
  }

  # create folders
  dir.create(qs$src_dir, recursive = TRUE)
  dir.create(qs$src_outdir)
  dir.create(qs$src_logdir)

  # save environment
  readr::write_rds(qsub_environment, qs$src_qsub_rds, compress = qs$compress)
  readr::write_rds(prism_environment, qs$src_prism_rds, compress = qs$compress)

  # write r script
  r_script <- paste0(
    "setwd(\"", qs$remote_dir, "\")\n",
    "PitSoL_index <- as.integer(commandArgs(trailingOnly=T)[[1]])\n",
    "PitSoL_file_out <- paste0(\"out/out_\", PitSoL_index, \".rds\", sep=\"\")\n",
    "if (!file.exists(PitSoL_file_out)) {\n",
    "  PitSoL_params <- readRDS(\"data_prism.rds\")\n",
    "  for (pack in PitSoL_params$PACKAGES) {\n",
    "    suppressMessages(library(pack, character.only = T))\n",
    "  }\n",
    "  set.seed(PitSoL_params$SEEDS[[PitSoL_index]])\n",
    "  list2env(as.list(readRDS(\"data_qsub.rds\")), environment())\n",
    "  PitSoL_out <-\n",
    "    lapply(\n",
    "      seq(PitSoL_params$QSUB_START[[PitSoL_index]], PitSoL_params$QSUB_STOP[[PitSoL_index]]),\n",
    "      function(PitSoL_data) {\n",
    "        do.call(PitSoL_params$FUN, c(list(PitSoL_params$X[[PitSoL_data]]), PitSoL_params$DOTPARAMS))\n",
    "      }\n",
    "    )\n",
    "  saveRDS(PitSoL_out, file = PitSoL_file_out, compress = PitSoL_params$COMPRESS)\n",
    "}\n"
  )
  readr::write_lines(r_script, qs$src_rfile)

  # write sh script
  sh_script <- paste0(
    "#!/bin/bash\n",
    ifelse(qs$num_cores == 1, "", paste0("#$ -pe serial ", qs$num_cores, "\n")),
    ifelse(is.numeric(qs$max_running_tasks), paste0("#$ -tc ", qs$max_running_tasks, "\n"), ""),
    "#$ -t 1-", qs$num_tasks, "\n",
    "#$ -N ", qs$name, "\n",
    "#$ -e ", qs$remote_dir, "/log/log.$TASK_ID.e.txt\n",
    "#$ -o ", qs$remote_dir, "/log/log.$TASK_ID.o.txt\n",
    "#$ -l h_vmem=", qs$memory, "\n",
    ifelse(!is.null(qs$max_wall_time), paste0("#$ -l h_rt=", qs$max_wall_time, "\n"), ""),
    "cd ", qs$remote_dir, "\n",
    "module unload R\n",
    ifelse(!is.null(qs$modules), paste0("module load ", qs$modules, "\n", collapse = "\n"), ""),
    paste0(paste0(qs$execute_before, collapse = "\n"), "\n"),
    "Rscript --default-packages=methods,stats,utils,graphics,grDevices script.R $SGE_TASK_ID\n"
  )
  readr::write_lines(sh_script, qs$src_shfile)

  # rsync local with remote
  mkdir_remote(
    path = qs$remote_tmp_path,
    remote = remote,
    verbose = qs$verbose
  )
  cp_remote(
    remote_src = NULL,
    path_src = qs$src_dir %>% str_replace_all("[\\/]$", ""),
    remote_dest = remote,
    path_dest = qs$remote_tmp_path %>% str_replace_all("[\\/]$", "")
  )

  invisible(NULL)
}

execute_job <- function(qsub_config) {
  # start job remotely and read job_id
  cmd <- glue::glue("cd {qsub_config$remote_dir}; qsub script.sh")

  output <- run_remote(cmd, remote = get_valid_remote_info(qsub_config), verbose = qsub_config$verbose)

  # retrieve job id
  job_id <- gsub(".*Your job-array ([0-9]*)[^\n]* has been submitted.*", "\\1", paste(output$stdout, collapse = "\n"))

  as.character(job_id)
}

#' Run a Function on a gridengine system!
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
  qsub_lapply(X = 1, function(i) FUN(), qsub_config = qsub_config, qsub_environment = qsub_environment, ...)[[1]]
}

#' Check whether a job is running.
#'
#' @param qsub_config The qsub configuration of class \code{qsub_configuration}, as returned by any qsub execution
#'
#' @export
is_job_running <- function(qsub_config) {
  if (!is.null(qsub_config$job_id)) {
    qstat_out <- run_remote("qstat", remote = get_valid_remote_info(qsub_config))$stdout
    any(str_detect(qstat_out, paste0("^ *", qsub_config$job_id, " ")))
  } else {
    FALSE
  }
}

#' Retrieve the results of a qsub execution.
#'
#' @param qsub_config The qsub configuration of class \code{qsub_configuration}, as returned by any qsub execution
#' @param wait If \code{TRUE}, wait until the execution has finished in order to return the results, else returns \code{NULL} if execution is not finished.
#' @param post_fun Apply a function to the output after execution. Interface: \code{function(index, output)}
#' @importFrom readr read_file
#' @importFrom pbapply pblapply
#' @export
qsub_retrieve <- function(qsub_config, wait = TRUE, post_fun = NULL) {
  # short hand notation
  qs <- qsub_config

  # commence SSH connection
  if (!is_valid_ssh_connection(qs$remote_ssh)) {
    remote_ssh <- create_ssh_connection(qs$remote)
    qs$remote_ssh <- remote_ssh
    on.exit(ssh::ssh_disconnect(remote_ssh))
  }

  if (is.logical(wait) && !wait && is_job_running(qsub_config)) {
    return(NULL)
  }

  if (!is.character(wait) || wait != "just_do_it") {
    while (is_job_running(qsub_config)) {
      if (qs$verbose) cat("Waiting for job ", qs$job_id, " to finish\n", sep = "")
      Sys.sleep(1)
    }
  }

  # copy results to local
  if (.Platform$OS.type == "windows") { # rsync is not supported on windows :(
    if (qs$verbose) cat("Cleaning up local dirs\n", sep = "")
    unlink(qs$src_logdir, recursive = TRUE)
    unlink(qs$src_outdir, recursive = TRUE)

    if (qs$verbose) cat("Downloading logs and outs\n", sep = "")
    cp_remote(remote_src = qs$remote_ssh, path_src = qs$remote_logdir, remote_dest = FALSE, path_dest = qs$src_dir)
    cp_remote(remote_src = qs$remote_ssh, path_src = qs$remote_outdir, remote_dest = FALSE, path_dest = qs$src_dir)
  } else {
    if (qs$verbose) cat("Downloading logs and outs\n", sep = "")
    rsync_remote(remote_src = qs$remote, path_src = qs$remote_logdir, remote_dest = FALSE, path_dest = qs$src_dir)
    rsync_remote(remote_src = qs$remote, path_src = qs$remote_outdir, remote_dest = FALSE, path_dest = qs$src_dir)
  }

  # read rds files
  if (qs$verbose) cat("Processing outs\n", sep = "")
  tryCatch({
    outs <-
      pbapply::pblapply(
        X = seq_len(qs$num_tasks),
        FUN = function(rds_i) {
          output_file <- paste0(qs$src_dir, "/out/out_", rds_i, ".rds")
          error_file <- paste0(qs$src_dir, "/log/log.", rds_i, ".e.txt")

          error_occurred <- TRUE

          if (file.exists(output_file)) {
            tryCatch({
              out_rds <- readRDS(output_file)
              error_occurred <- FALSE
            }, error = function(e) {

            })
          }
          if (!error_occurred) {
            if (!is.null(post_fun)) {
              ixs <- (rds_i - 1) * qs$batch_tasks + seq_along(out_rds)
              out_rds <- map2(ixs, out_rds, post_fun)
            }
            out_rds
          } else {
            if (file.exists(error_file)) {
              msg <- readr::read_file(error_file)
              txt <- paste0("File: ", error_file, "\n", msg)
            } else {
              txt <- paste0(
                "File: ", error_file, "\n",
                "No output or log file found. Either the job did not run, or it ran out of time or memory.\n",
                "Check 'qacct -j ", qs$job_id, "' for more info."
              )
            }

            if (qs$stop_on_error) {
              stop(txt)
            } else {
              map(seq_len(qs$batch_tasks), function(x) {
                out <- NA
                attr(out, "qsub_error") <- txt
                out
              })
            }
          }
        }
      ) %>%
      unlist(recursive = FALSE)
  }, error = function(e) {
    stop(e)
  }, finally = {
    # remove temporary folders afterwards
    if (qs$remove_tmp_folder) {
      run_remote(
        paste0("rm -rf \"", qs$remote_dir, "\""),
        remote = qs$remote_ssh,
        verbose = qs$verbose
      )
      unlink(qs$src_dir, recursive = TRUE, force = TRUE)
    }
  })

  return(outs)
}

#' @importFrom ssh ssh_info
is_valid_ssh_connection <- function(ssh_connection) {
  if (is.null(ssh_connection) || !is(ssh_connection, "ssh_session"))
    return(FALSE)

  tryCatch({
    info <- ssh::ssh_info(ssh_connection)
    TRUE
  }, error = function(e) {
    FALSE
  })
}

get_valid_remote_info <- function(qsub_config) {
  if (is_valid_ssh_connection(qsub_config$remote_ssh)) {
    qsub_config$remote_ssh
  } else {
    qsub_config$remote
  }
}
