#' Create a qsub configuration object.
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server}
#'   that does not require interactive password entry.
#' @param local_tmp_path A directory on the local machine in which to store temporary files. Should not contain a tilde ('~').
#' @param remote_tmp_path A directory on the remote machine in which to store temporary files. Should not contain a tilde ('~').
#' @param name The name of the execution
#' @param num_cores The number of cores to allocate per task
#' @param max_running_tasks öimit concurrent array job task execution
#' @param memory The memory to allocate per core
#' @param verbose Print out any ssh commands
#' @param wait If \code{TRUE}, will wait until the execution has finished by periodically checking the job status.
#' @param remove_tmp_folder If \code{TRUE}, will remove everything that was created related to this execution at the end.
#' @param r_module The R module to use
#' @param stop_on_error If \code{TRUE}, will stop when an error occurs, else returns a NA for errored instances.
#' @param max_tasks The maximum number of tasks to spawn
#' @param execute_before Commands to execute in the shell before running R
#'
#' @importFrom random randomStrings
#'
#' @return A qsub configuration object.
#'
#' @export
#'
#' @seealso \code{\link{qsub_lapply}}, \code{\link{set_default_qsub_config}}
#'
#' @examples
#' \dontrun{
#' qsub_config <- create_qsub_config(
#'   remote = "myserver",
#'   local_tmp_path = "/home/myuser/workspace/.r2gridengine",
#'   remote_tmp_path = "/scratch/myuser/.r2gridengine"
#' )
#' qsub_lapply(1:10, function(x) x + 1, qsub_config = qsub_config)
#'
#' set_default_qsub_config(qsub_config, permanent = T)
#' qsub_lapply(1:10, function(x) x + 1)
#' }
create_qsub_config <- function(
  remote,
  local_tmp_path,
  remote_tmp_path,
  r_module = "R",
  name = "R2PRISM",
  num_cores = 1,
  max_running_tasks = NULL,
  memory = "4G",
  verbose = F,
  wait = T,
  remove_tmp_folder = T,
  stop_on_error = T,
  max_tasks = NULL,
  execute_before = NULL
) {
  qsub_conf <- formals(create_qsub_config)
  new_values <- as.list(match.call())[-1]
  qsub_conf[names(new_values)] <- new_values
  qsub_conf <- lapply(qsub_conf, eval)
  class(qsub_conf) <- "PRISM::qsub_config"
  qsub_conf
}

is_qsub_config <- function(object) {
  class(object) == "PRISM::qsub_config"
}

test_qsub_config <- function(object) {
  if (!is_qsub_config(object)) {
    stop(sQuote("qsub_config"), " needs to be a valid qsub_config object. See ", sQuote("create_qsub_config"), " for more details.")
  }
}

#' Set a default qsub_config.
#'
#' @description
#' If permanent, the qsub_config will be written to the specified path.
#' Otherwise, it will be saved in the current environment.
#'
#' @usage
#' set_default_qsub_config(qsub_config, permanent = T, permanent_file = "~/.local/share/R2PRISM/qsub_config.rds")
#'
#' @param qsub_config The qsub_config to use as default.
#' @param permanent Whether or not to make this the default qsub_config.
#' @param permanent_file The location to which to save the permanent qsub_config.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' qsub_config <- create_qsub_config(
#'   remote = "myserver",
#'   local_tmp_path = "/home/myuser/workspace/.r2gridengine",
#'   remote_tmp_path = "/scratch/myuser/.r2gridengine"
#' )
#' set_default_qsub_config(qsub_config, permanent = T)
#' qsub_lapply(1:10, function(x) x + 1)
#' }
#'
#' @seealso \code{\link{qsub_lapply}}, \code{\link{create_qsub_config}}
set_default_qsub_config <- function(qsub_config, permanent = T, permanent_file = "~/.local/share/R2PRISM/qsub_config.rds") {
  if (is.null(qsub_config)) {
    rm(.default_qsub_config)
    if (permanent) {
      file.remove(permanent_file)
    }
  } else {
    test_qsub_config(qsub_config)
    .default_qsub_config <- qsub_config
    if (permanent) {
      saveRDS(qsub_config, permanent_file)
    }
  }
}

#' Retrieve a default qsub_config.
#'
#' @description
#' Will prefer the temporary default over the permanent default.
#' You should typically not require this function.
#'
#' @usage
#' get_default_qsub_config(permanent_file = "~/.local/share/R2PRISM/qsub_config.rds")
#'
#' @param permanent_file The file in which a permanent default config is stored.
#'
#' ## @export # you should typically not require to call this function manually.
get_default_qsub_config <- function(permanent_file = "~/.local/share/R2PRISM/qsub_config.rds") {
  if (".default_qsub_config" %in% ls(all.names = T)) {
    .default_qsub_config
  } else if (file.exists(permanent_file)) {
    readRDS(permanent_file)
  } else {
    stop("No default qsub_config could be found. Did you run ", sQuote("set_default_qsub_config"), " yet?")
  }
}

#' Create an instance of the qsub_config.
#'
#' @description
#' This function generates the paths for the temporary files.
#'
#' @param qsub_config A valid qsub_config object.
#'
#' ## @export # you should typically not require to call this function manually.
instantiate_qsub_config <- function(qsub_config) {
  test_qsub_config(qsub_config)

  tmp_foldername <- paste0(
    format(Sys.time(), "%Y%m%d_%H%M%S"), "_",
    qsub_config$name, "_",
    random::randomStrings(n = 1, len = 10)[1,])

  src_dir <- paste0(qsub_config$local_tmp_path, "/", tmp_foldername)
  remote_dir <- paste0(qsub_config$remote_tmp_path, "/", tmp_foldername)

  qsub_conf <- c(qsub_config, list(
    src_dir = src_dir,
    src_outdir = paste0(src_dir, "/out"),
    src_logdir = paste0(src_dir, "/log"),
    src_rdata = paste0(src_dir, "/data.RData"),
    src_rfile = paste0(src_dir, "/script.R"),
    src_shfile = paste0(src_dir, "/script.sh"),
    remote_dir = remote_dir,
    remote_outdir = paste0(remote_dir, "/out"),
    remote_logdir = paste0(remote_dir, "/log"),
    remote_rdata = paste0(remote_dir, "/data.RData"),
    remote_rfile = paste0(remote_dir, "/script.R"),
    remote_shfile = paste0(remote_dir, "/script.sh")
  ))
}

#' Create a new qsub configuration object from an old qsub configuration.
#'
#' @usage
#' override_qsub_config(qsub_config = NULL, ...)
#'
#' @param qsub_config The qsub_config to be overridden. If NULL, will attempt to retrieve a default qsub_config.
#' @param ... Any parameters to be overridden.
#'
#' @export
#'
#' @return A new qsub_config object.
#'
#' @seealso \code{\link{set_default_qsub_config}}
#'
#' @examples
#' \dontrun{
#' qsub_config <- create_qsub_config(
#'   remote = "myserver",
#'   local_tmp_path = "/home/myuser/workspace/.r2gridengine",
#'   remote_tmp_path = "/scratch/myuser/.r2gridengine"
#' )
#' qsub_lapply(1:10, function(x) x + 1, qsub_config = qsub_config)
#'
#' qsub_config2 <- override_qsub_config(qsub_config, remote = "yourserver")
#' qsub_lapply(1:10, function(x) x + 1, qsub_config = qsub_config)
#' }
override_qsub_config <- function(qsub_config = NULL, ...) {
  if (is.null(qsub_config)) {
    qsub_config <- get_default_qsub_config()
  }
  test_qsub_config(qsub_config)

  param_names <- names(formals(PRISM::create_qsub_config))

  new_values <- list(...)
  new_values <- new_values[names(new_values) %in% param_names]
  old_values <- qsub_config[names(new_values) %in% param_names & !names(qsub_config) %in% names(new_values)]

  do.call(create_qsub_config, c(new_values, old_values))
}


