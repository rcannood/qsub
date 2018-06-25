#' Create a qsub configuration object.
#'
#' @usage
#' create_qsub_config(
#'   # server settings
#'   remote,
#'   local_tmp_path,
#'   remote_tmp_path,
#'
#'   # execution parameters
#'   name = "r2qsub",
#'   num_cores = 1,
#'   memory = "4G",
#'   max_running_tasks = NULL,
#'   max_wall_time = "01:00:00",
#'
#'   # pre-execution parameters
#'   r_module = "R",
#'   execute_before = NULL,
#'   verbose = FALSE,
#'
#'   # post-execution parameters
#'   wait = TRUE,
#'   remove_tmp_folder = TRUE,
#'   stop_on_error = TRUE
#' )
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server:port}
#'   that does not require interactive password entry.
#' @param local_tmp_path A directory on the local machine in which to store temporary files. Should not contain a tilde ('~').
#' @param remote_tmp_path A directory on the remote machine in which to store temporary files. Should not contain a tilde ('~').
#'
#' @param name The name of the execution. This will show up, for instance, in \code{qstat}.
#' @param num_cores The number of cores to allocate per element in \code{X} in a \code{\link{qsub_lapply}} (default: \code{1}).
#' @param memory The memory to allocate per core (default: \code{"4G"}).
#'   If this is set too high without it being required, you might not be able to make optimal use of the remote cluster.
#' @param max_running_tasks limit concurrent array job task execution (default: \code{NULL}, infinite).
#'   If you have long jobs and there are many other users on the cluster,
#'   it is recommended you set this value to a reasonable number, such as 1/4th the total number of nodes * number of cores per node.
#' @param max_wall_time The maximum time each task is allowed to run (default: \code{"01:00:00"}, 1 hour).
#'   If set to \code{NULL}, the job will be allowed to run indefinitely.
#'   Mind you, this might annoy other users of the cluster.
#'
#' @param r_module The R module to use (default: \code{"R"}). If set to \code{NULL}, it will be assumed Rscript will be available in the path through other means.
#' @param execute_before Commands to execute in the bash shell before running R. For instance, you might need to load other modules such as gcc or python.
#' @param verbose Whether or not to print out any ssh commands.
#'
#' @param wait If \code{TRUE}, will wait until the execution has finished by periodically checking the job status.
#' @param remove_tmp_folder If \code{TRUE}, will remove everything that was created related to this execution at the end.
#' @param stop_on_error If \code{TRUE}, will stop when an error occurs, else returns a NA for errored instances.
#'
#' @importFrom random randomStrings
#' @importFrom methods formalArgs
#'
#' @return A qsub configuration object.
#'
#' @export
#'
#' @seealso \code{\link{qsub_lapply}}, \code{\link{set_default_qsub_config}}
#'
#' @rdname create_qsub_config
#'
#' @examples
#' \dontrun{
#' qsub_config <- create_qsub_config(
#'   remote = "myuser@myserver.mylocation.com:22",
#'   local_tmp_path = "/home/myuser/workspace/.r2gridengine",
#'   remote_tmp_path = "/scratch/myuser/.r2gridengine"
#' )
#' qsub_lapply(1:10, function(x) x + 1, qsub_config = qsub_config)
#'
#' set_default_qsub_config(qsub_config, permanent = TRUE)
#' qsub_lapply(1:10, function(x) x + 1)
#'
#' qsub_lapply(
#'   X = 1:10,
#'   FUN = function(x) x + 1,
#'   qsub_config = override_qsub_config(verbose = TRUE)
#' )
#' }
create_qsub_config <- function(
  # server settings
  remote,
  local_tmp_path,
  remote_tmp_path,

  # execution parameters
  name = "r2qsub",
  num_cores = 1,
  memory = "4G",
  max_running_tasks = NULL,
  max_wall_time = "01:00:00",

  # pre-execution parameters
  r_module = "R",
  execute_before = NULL,
  verbose = FALSE,
  # use_cpulimit = TRUE,

  # post-execution parameters
  wait = TRUE,
  remove_tmp_folder = TRUE,
  stop_on_error = TRUE
) {
  test <- c(remote, local_tmp_path, remote_tmp_path)
  qsub_conf <- as.list(environment())
  qsub_conf <- qsub_conf[intersect(names(qsub_conf), methods::formalArgs(create_qsub_config))]
  class(qsub_conf) <- c(class(qsub_conf), "qsub::qsub_config")
  qsub_conf
}

#' Returns whether the passed object is a qsub_config object.
#'
#' @param object The object to be tested
#'
#' @export
is_qsub_config <- function(object) {
  "qsub::qsub_config" %in% class(object)
}

#' Tests whether the passed object is a qsub_config object.
#'
#' @param object The object to be tested
#'
#' @export
test_qsub_config <- function(object) {
  if (!is_qsub_config(object)) {
    stop(sQuote("qsub_config"), " needs to be a valid qsub_config object. See ", sQuote("create_qsub_config"), " for more details.")
  }
}

config_file_location <- function() {
  if (.Platform$OS.type == "unix") {
    "~/.local/share/qsub/qsub_config.rds"
  } else if (.Platform$OS.type == "windows") {
    "~/../AppData/Local/qsub/qsub_config.rds"
  }
}

#' Set a default qsub_config.
#'
#' If permanent, the qsub_config will be written to the specified path.
#' Otherwise, it will be saved in the current environment.
#'
#' @param qsub_config The qsub_config to use as default.
#' @param permanent Whether or not to make this the default qsub_config.
#' @param config_file The location to which to save the permanent qsub_config.
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
set_default_qsub_config <- function(
  qsub_config,
  permanent = TRUE,
  config_file = config_file_location()
) {
  if (is.null(qsub_config)) {
    options("qsub_config" = NULL)
    if (permanent) {
      file.remove(config_file)
    }
  } else {
    test_qsub_config(qsub_config)
    options("qsub_config" = qsub_config)

    # if save is permanent
    if (permanent) {
      # create parent folder, if necessary
      folder <- gsub("[^\\/]*$", "", config_file)
      if (!file.exists(folder)) {
        dir.create(folder, recursive = TRUE)
      }

      # save file at desired location
      saveRDS(qsub_config, config_file)
    }
  }
}

#' Retrieve a default qsub_config.
#'
#' @description
#' Will prefer the temporary default over the permanent default.
#' You should typically not require this function.
#'
#' @param config_file The file in which a permanent default config is stored.
#'
#' @export
get_default_qsub_config <- function(
  config_file = config_file_location()
) {
  opt <- getOption("qsub_config")
  if (!is.null(opt)) {
    opt
  } else if (file.exists(config_file)) {
    readRDS(config_file)
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
    src_qsub_rds = paste0(src_dir, "/data_qsub.rds"),
    src_prism_rds = paste0(src_dir, "/data_prism.rds"),
    src_rfile = paste0(src_dir, "/script.R"),
    src_shfile = paste0(src_dir, "/script.sh"),
    remote_dir = remote_dir,
    remote_outdir = paste0(remote_dir, "/out"),
    remote_logdir = paste0(remote_dir, "/log"),
    remote_qsub_rds = paste0(remote_dir, "/data_qsub.rds"),
    remote_prism_rds = paste0(remote_dir, "/data_prism.rds"),
    remote_rfile = paste0(remote_dir, "/script.R"),
    remote_shfile = paste0(remote_dir, "/script.sh")
  ))

  qsub_conf
}

#' @rdname create_qsub_config
#'
#' @param qsub_config A qsub_config to be overridden
#'
#' @importFrom methods formalArgs
#'
#' @export
#'
#' @usage
#' override_qsub_config(
#'   qsub_config = get_default_qsub_config(),
#'
#'   # server settings
#'   remote = qsub_config$remote,
#'   local_tmp_path = qsub_config$local_tmp_path,
#'   remote_tmp_path = qsub_config$remote_tmp_path,
#'
#'   # execution parameters
#'   name = qsub_config$name,
#'   num_cores = qsub_config$num_cores,
#'   memory = qsub_config$memory,
#'   max_running_tasks = qsub_config$max_running_tasks,
#'   max_wall_time = qsub_config$max_wall_time,
#'
#'   # pre-execution parameters
#'   r_module = qsub_config$r_module,
#'   execute_before = qsub_config$execute_before,
#'   verbose = qsub_config$verbose,
#'
#'   # post-execution parameters
#'   wait = qsub_config$wait,
#'   remove_tmp_folder = qsub_config$remove_tmp_folder,
#'   stop_on_error = qsub_config$stop_on_error
#' )
override_qsub_config <- function(
  qsub_config = get_default_qsub_config(),

  # server settings
  remote = qsub_config$remote,
  local_tmp_path = qsub_config$local_tmp_path,
  remote_tmp_path = qsub_config$remote_tmp_path,

  # execution parameters
  name = qsub_config$name,
  num_cores = qsub_config$num_cores,
  memory = qsub_config$memory,
  max_running_tasks = qsub_config$max_running_tasks,
  max_wall_time = qsub_config$max_wall_time,

  # pre-execution parameters
  r_module = qsub_config$r_module,
  execute_before = qsub_config$execute_before,
  verbose = qsub_config$verbose,

  # post-execution parameters
  wait = qsub_config$wait,
  remove_tmp_folder = qsub_config$remove_tmp_folder,
  stop_on_error = qsub_config$stop_on_error
) {
  test_qsub_config(qsub_config)
  old_values <- qsub_config

  qsub_config_param_names <- methods::formalArgs(create_qsub_config)

  new_values <- as.list(environment())
  new_values <- new_values[intersect(names(new_values), qsub_config_param_names)]
  old_values <- old_values[setdiff(intersect(names(old_values), qsub_config_param_names), names(new_values))]

  do.call(create_qsub_config, c(new_values, old_values))
}


