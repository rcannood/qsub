parse_remote <- function(remote) {
  if (is.logical(remote)) {
    if (remote) {
      remote <- get_default_qsub_config()$remote
    } else {
      return(list(local = TRUE))
    }
  }

  if (!is.character(remote)) {
    stop("remote must be FALSE, TRUE, 'user@ipaddress:port', or a Host listed in your ~/.ssh/config.")
  }

  # retrieve the host name by removing
  # the specified user and/or port, if present
  hostname <-
    remote %>%
    str_replace(".*@", "") %>%
    str_replace(":.*", "")

  # use default port 22 if none is specified
  port <- "22"

  # if hostname is listed in the .ssh config, use that
  config_data <- fetch_hostname_from_config(hostname)
  if (!is.null(config_data$hostname)) hostname <- config_data$hostname
  if (!is.null(config_data$username)) username <- config_data$username
  if (!is.null(config_data$port)) port <- config_data$port

  # if a specific username or port is detected, use that instead
  if (str_detect(remote, "@")) username <- str_replace(remote, "@.*", "")
  if (str_detect(remote, ":")) port <- str_replace(remote, ".*:", "")

  list(
    local = FALSE,
    hostname = hostname,
    username = username,
    port = port
  )
}

#' Create an SSH connection with remote
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For the default qsub config remote, use \code{TRUE}.
create_ssh_connection <- function(remote) {
  remote_info <- parse_remote(remote)

  if (remote_info$local) {
    stop("Cannot create an SSH connection to the local machine.")
  }
  remote_string <- with(remote_info, paste0(username, "@", hostname, ":", port))

  ssh::ssh_connect(remote_string)
}

#' \code{run_remote} - Runs the command locally or remotely using ssh.
#'
#' In \code{run_remote} the remote commands are enclosed in wrappers that allow to capture output.
#' By default stderr is redirected to stdout.
#' If there's a genuine error, e.g., the remote command does not exist, the output is not captured. In this case, one can
#' see the output by setting \code{intern} to \code{FALSE}. However, when the command is run but exits with non-zero code,
#' \code{run_remote} intercepts the generated warning and saves the output.
#'
#' The remote command will be put inside double quotes twice, so all quotes in cmd must be escaped twice: \code{\\\"}.
#' However, if the command is not remote, i.e., \code{remote} is \code{NULL} or empty string, quotes should be escaped
#' only once.
#'
#' If the command itself redirects output, the \code{stderr_redirect} flag should be set to \code{FALSE}.
#' @param command Command to run. If run locally, quotes should be escaped once.
#'        If run remotely, quotes should be escaped twice.
#' @param args Character vector, arguments to the command.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass \code{FALSE} (default). For
#'        execution on the default qsub config remote, use \code{TRUE}.
#' @param verbose If \code{TRUE} prints the command.
#' @param shell Whether to execute the command in a shell
#' @return A list with components:
#' \itemize{
#' \item \code{status} The exit status of the process. If this is NA, then the process was killed and had no exit status.
#' \item \code{stdout} The standard output of the command, in a character scalar.
#' \item \code{stderr} The standard error of the command, in a character scalar.
#' \item \code{elapsed_time} The number of seconds required before this function returned an output.
#' }
#' Warnings are really errors here so the error flag is set if there are warnings.
#'
#' @importFrom ssh ssh_exec_internal ssh_connect ssh_disconnect
#' @importFrom processx run
#'
#' @export
run_remote <- function(command, remote = FALSE, args = character(), verbose = FALSE, shell = FALSE) {
  if (verbose) cat("# ", gsub("\n", "\n# ", command), " ", paste(args, collapse = " "), "\n", sep = "")

  time1 <- Sys.time()

  if (!is_remote_local(remote)) {
    if (!is_valid_ssh_connection(remote)) {
      conn <- create_ssh_connection(remote)
      on.exit(ssh::ssh_disconnect(conn))
    } else {
      conn <- remote
    }

    cmd <- paste0( # see https://stackoverflow.com/a/1472444
      "source /etc/profile;",
      "if [[ -s \"$HOME/.bash_profile\" ]]; then",
      "  source \"$HOME/.bash_profile\";",
      "fi;",
      "if [[ -s \"$HOME/.profile\" ]]; then",
      "  source \"$HOME/.profile\";",
      "fi;",
      command, " ", paste(args, collapse = " ")
    )
    cmd_out <- ssh::ssh_exec_internal(session = conn, command = cmd, error = FALSE)
    cmd_out$stdout <- rawToChar(cmd_out$stdout) %>% strsplit("\n") %>% first()
    cmd_out$stderr <- rawToChar(cmd_out$stderr) %>% strsplit("\n") %>% first()
  } else {
    if (shell) {
      args <- c("-c", paste0(command, " ", paste0(args, collapse = " ")))
      command <- "bash"
    }

    cmd_out <- processx::run(
      command = command,
      args = args,
      error_on_status = FALSE,
      echo_cmd = verbose
    )
  }

  time2 <- Sys.time()

  cmd_out$elapsed_time <- as.numeric(time2 - time1, units = "secs")

  cmd_out
}

#' A wrapper around the scp shell command that handles local/remote files and allows
#' copying between remote hosts via the local machine.
#'
#' @param remote_src Remote machine for the source file in the format \code{user@@machine} or an empty string for local.
#' @param path_src Path of the source file.
#' @param remote_dest Remote machine for the destination file in the format \code{user@@machine} or an empty string for local.
#' @param path_dest Path for the source file; can be a directory.
#' @param verbose Prints elapsed time if TRUE
#' @param recursively Copy a directory recursively?
#'
#' @export
#'
#' @examples
#' \dontrun{
#' ## Copy file myfile.csv from the home directory on the remote server to
#' ## the local working directory.
#'
#' ## on remote server in bash shell:
#' # cat myfile.csv
#' # [me@@myserver ~]$ cat myfile.csv
#' # "val","ts"
#' # 1,
#' # 2,
#' # 3,
#' # 4,
#' # 5,
#' # 6,
#' # 7,
#' # 8,
#' # 9,
#' # 10,
#'
#' ## on local server in R:
#' cp_remote(remote_src = "me@@myserver", path_src = "~/myfile.csv",
#'           remote_dest = FALSE, path_dest = getwd(), verbose = TRUE)
#' # [1] "Elapsed: 1.672 sec"
#' df <- read.csv("myfile.csv")
#' df
#' #    val ts
#' # 1    1 NA
#' # 2    2 NA
#' # 3    3 NA
#' # 4    4 NA
#' # 5    5 NA
#' # 6    6 NA
#' # 7    7 NA
#' # 8    8 NA
#' # 9    9 NA
#' # 10  10 NA
#' }
cp_remote <- function(
  remote_src,
  path_src,
  remote_dest,
  path_dest,
  verbose = FALSE,
  recursively = FALSE
) {
  if (!is_remote_local(remote_src) && !is_valid_ssh_connection(remote_src)) {
    remote_src <- create_ssh_connection(remote_src)
    on.exit(ssh::ssh_disconnect(remote_src))
  }

  if (!is_remote_local(remote_dest) && !is_valid_ssh_connection(remote_dest)) {
    remote_dest <- create_ssh_connection(remote_dest)
    on.exit(ssh::ssh_disconnect(remote_dest))
  }

  if (!is_remote_local(remote_src) && !is_remote_local(remote_dest)) {
    # BOTH ARE NOT LOCAL
    path_tmp <- tempfile(pattern = "tmp_scp_")
    on.exit(unlink(path_tmp, recursive = TRUE, force = TRUE))

    ssh::scp_download(session = remote_src, files = path_src, to = path_tmp, verbose = verbose)
    ssh::scp_upload(session = remote_dest, files = path_tmp, to = path_dest, verbose = verbose)

  } else if (is_remote_local(remote_src)) {
    # SRC IS LOCAL; DEST IS REMOTE
    ssh::scp_upload(session = remote_dest, files = path_src, to = path_dest, verbose = verbose)

  } else if (is_remote_local(remote_dest)) {
    # DEST IS LOCAL; SRC IS REMOTE
    ssh::scp_download(session = remote_src, files = path_src, to = path_dest, verbose = verbose)

  } else {
    # BOTH ARE LOCAL
    file.copy(path_src, path_dest, recursive = TRUE)
  }

}

#' Rsync files between machines
#'
#' A wrapper around the rsync shell command that allows copying between remote hosts via the local machine.
#'
#' @param remote_src Remote machine for the source, see the section below 'Specifying a remote'.
#' @param path_src Path of the source file.
#' @param remote_dest Remote machine for the destination, see the section below 'Specifying a remote'.
#' @param path_dest Path for the source file; can be a directory.
#' @param compress Whether or not to compress the data being transferred.
#' @param delete Whether or not to delete files at the target remote. Use \code{"yes"} to delete files at the remote.
#' @param exclude A vector of files / regexs to be excluded.
#' @param verbose Prints elapsed time if TRUE.
#'
#' @section Specifying a remote:
#' A remote can be specified in one of the following ways:
#' \itemize{
#'   \item{A character vector in format \code{user@@ipaddress:port},}
#'   \item{The name of a Host in the \code{~/.ssh/config} file,}
#'   \item{\code{FALSE} for the local machine,}
#'   \item{\code{TRUE} for the default remote specified as \code{get_default_qsub_config()$remote}.}
#' }
#'
#' @export
rsync_remote <- function(
  remote_src,
  path_src,
  remote_dest,
  path_dest,
  compress = TRUE,
  delete = "no",
  exclude = NULL,
  verbose = FALSE
) {
  if (.Platform$OS.type == "windows") {
    stop("rsync_remote is not implemented for Windows systems")
  }

  remote_src_info <- parse_remote(remote_src)
  remote_dest_info <- parse_remote(remote_dest)

  if (!remote_src_info$local) {
    path_src <- with(remote_src_info, c(
      "-e",
      paste0("'ssh -p ", port, "'"),
      paste0(username, "@", hostname, ":", path_src)
    ))
  }
  if (!remote_dest_info$local) {
    path_dest <- with(remote_dest_info, c(
      "-e",
      paste0("'ssh -p ", port, "'"),
      paste0(username, "@", hostname, ":", path_dest)
    ))
  }

  if (!is.null(exclude)) {
    exclude_str <- glue("--exclude={exclude}")
  } else {
    exclude_str <- ""
  }

  flags <- paste0(
    "-av",
    ifelse(compress, "z", "")
  )

  if (identical(delete, TRUE)) {
    warning("Specify `delete = \"yes\"` in order to delete files at the target remote.")
  }

  delete_flag <-
    if (delete == "yes") {
      delete_flag <- "--delete"
    } else {
      delete_flag <- NULL
    }

  res <- run_remote(
    "rsync",
    args = c(flags, path_src, path_dest, exclude_str, delete_flag),
    remote = FALSE,
    verbose = verbose,
    shell = TRUE
  )

  if (res$stderr != "") {
    stop(paste0("rsync failed: ", res$stderr))
  }

  if (verbose) print(paste("Elapsed:", res$elapsed_time, "sec"))
}

#' Checks if a local or remote file exists.
#'
#' @param file File path.
#' @inheritParams run_remote
#'
#' @return \code{TRUE} or \code{FALSE} indicating whether the file exists.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' file_exists_remote("~/myfile.csv", remote = "me@@myserver")
#' # [1] TRUE
#' }
file_exists_remote <- function(file, remote = FALSE, verbose = FALSE) {
  if (is_remote_local(remote)) {
    file.exists(file)
  } else { # assume remote is unix based
    cmd <- glue::glue("(ls {file} >> /dev/null 2>&1 && echo TRUE) || echo FALSE")
    run_remote(cmd, remote = remote, verbose = verbose, shell = TRUE)$stdout %>% paste0(collapse = " ") %>% str_replace(".*(TRUE|FALSE)$", "\\1") %>% as.logical()
  }
}

#' Creates a remote directory with the specified group ownership and permissions.
#'
#' @param path Directory path. If using \code{remote}, this should be a full path or
#'             a path relative to the user's home directory.
#' @inheritParams run_remote
#'
#' @export
mkdir_remote <- function(path, remote = FALSE, verbose = FALSE) {
  if (is_remote_local(remote)) {
    if (!file_exists_remote(path, remote)) {
      dir.create(path = path, recursive = TRUE, showWarnings = verbose)
    }
  } else {
    if (!file_exists_remote(path, remote)) {
      run_remote("mkdir", args = c("-p", path), remote = remote, verbose = verbose)
    }
  }

  invisible()
}

#' Read from a file remotely
#'
#' @param path Path of the file.
#' @inheritParams run_remote
#'
#' @importFrom readr read_file
#'
#' @export
cat_remote <- function(path, remote = FALSE, verbose = FALSE) {
  if (is_remote_local(remote)) {
    readr::read_file(path)
  } else {
    output <- run_remote("cat", args = path, remote = remote, verbose = verbose)

    if (length(output$stderr) > 0 && output$stderr[[1]] != "") {
      stop(output$stderr)
    }

    output$stdout
  }
}

#' Write to a file remotely
#'
#' @param x The text to write to the file.
#' @param path Path of the file.
#' @inheritParams run_remote
#'
#' @importFrom readr write_lines
#'
#' @export
write_remote <- function(x, path, remote = FALSE, verbose = FALSE) {
  if (is_remote_local(remote)) {
    readr::write_lines(x, path)
  } else {
    tmpfile <- tempfile()
    readr::write_lines(x, tmpfile)

    if (!is(remote, "ssh_session")) {
      remote <- create_ssh_connection(remote)
      on.exit(ssh::ssh_disconnect(remote))
    }

    cp_remote(
      remote_src = FALSE,
      path_src = tmpfile,
      remote_dest = path,
      path_dest = remote,
      verbose = verbose
    )
  }
}

#' View the contents of a directory remotely
#'
#' @param path Path of the directory.
#' @inheritParams run_remote
#'
#' @export
ls_remote <- function(path, remote = FALSE, verbose = FALSE) {
  if (is_remote_local(remote)) {
    list.files(path)
  } else {
    run_remote("ls", args = c("-1", path), remote = remote, verbose = verbose)$stdout
  }
}

#' Show the status of Grid Engine jobs and queues
#'
#' @inheritParams run_remote
#'
#' @export
qstat_remote <- function(remote = NULL, verbose = FALSE) {
  run_remote("qstat", remote = remote, verbose = verbose)$stdout
}


#' Tests whether the remote is a local host or not.
#'
#' @param remote A putative remote machine. This function will return true if \code{remote == FALSE}.
is_remote_local <- function(remote) {
  !is(remote, "ssh_session") && (is.null(remote) || is.na(remote) || (is.logical(remote) && !remote))
}

#' Remove a file or folder
#' @param path Path of the file/folder
#' @inheritParams run_remote
#' @param recursive Whether to work recursively
#' @param force Whether to force removal
#' @export
rm_remote <- function(path, remote, recursive = FALSE, force = FALSE, verbose = FALSE) {
  if (is_remote_local(remote)) {
    unlink(path, recursive = recursive)
  } else {
    run_remote(
      glue("rm {path} {ifelse(recursive, ' -r ', '')} {ifelse(force, ' -f ', '')}"),
      remote = remote,
      verbose = verbose
    )$stdout
  }
}
