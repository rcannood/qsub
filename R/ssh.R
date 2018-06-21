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
#' @param cmd Command to run. If run locally, quotes should be escaped once.
#'        If run remotely, quotes should be escaped twice.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
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
run_remote <- function(cmd, remote, verbose = FALSE) {
  if (verbose) cat("# ", gsub("\n", "\n# ", cmd), "\n", sep="")

  time1 <- Sys.time()

  if (!is_remote_local(remote) && !is(remote, "ssh_session")) {
    remote <- ssh::ssh_connect(remote)
    on.exit(ssh::ssh_disconnect(remote))
  }

  if (is(remote, "ssh_session")) {
    cmd2 <- paste0("source /etc/profile.d/modules.sh; source /etc/profile.d/sge.sh; ", cmd)
    cmd_out <- ssh::ssh_exec_internal(session = remote, command = cmd2, error = FALSE)
    cmd_out$stdout <- rawToChar(cmd_out$stdout) %>% strsplit("\n") %>% .[[1]]
    cmd_out$stderr <- rawToChar(cmd_out$stderr) %>% strsplit("\n") %>% .[[1]]
  } else {
    cmd_out <- processx::run(
      command = sub(" .*$", "", cmd),
      args = sub("^[^ ]* ?", "", cmd),
      error_on_status = FALSE
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
#'           remote_dest = "", path_dest = getwd(), verbose = TRUE)
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
  if (!is_remote_local(remote_src) && !is(remote_src, "ssh_session")) {
    remote_src <- ssh::ssh_connect(remote_src)
    on.exit(ssh::ssh_disconnect(remote_src))
  }

  if (!is_remote_local(remote_dest) && !is(remote_dest, "ssh_session")) {
    remote_dest <- ssh::ssh_connect(remote_dest)
    on.exit(ssh::ssh_disconnect(remote_dest))
  }


  if (!is_remote_local(remote_src) && !is_remote_local(remote_dest)) {
    # BOTH ARE NOT LOCAL
    path_tmp <- tempfile(pattern = "tmp_scp_", tmpdir = local_temp_dir)
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

#' A wrapper around the rsync shell command that allows copying between remote hosts via the local machine.
#'
#' @param remote_src Remote machine for the source file in the format \code{user@@machine} or an empty string for local.
#' @param path_src Path of the source file.
#' @param remote_dest Remote machine for the destination file in the format \code{user@@machine} or an empty string for local.
#' @param path_dest Path for the source file; can be a directory.
#' @param excluse A vector of files / regexs to be excluded
#' @param verbose Prints elapsed time if TRUE
#'
#' @importFrom glue glue
#'
#' @export
rsync_remote <- function(remote_src, path_src, remote_dest, path_dest, exclude = NULL, verbose = FALSE) {
  if (.Platform$OS.type == "windows") {
    stop("rsync_remote is not implemented for Windows systems")
  }

  if (!is_remote_local(remote_src)) {
    path_src <- glue("-e ssh {remote_src}:{path_src}")

    if (!is(remote_src, "ssh_session")) {
      remote_src <- ssh::ssh_connect(remote_src)
      on.exit(ssh::ssh_disconnect(remote_src))
    }
  }
  if (!is_remote_local(remote_dest)) {
    path_dest <- glue("-e ssh {remote_dest}:{path_dest}")

    if (!is(remote_dest, "ssh_session")) {
      remote_dest <- ssh::ssh_connect(remote_dest)
      on.exit(ssh::ssh_disconnect(remote_dest))
    }
  }

  if (!is.null(exclude)) {
    exclude_str <- paste(glue(" --exclude={exclude} "), collapse = "")
  } else {
    exclude_str <- ""
  }

  command <- glue("rsync -avz {path_src} {path_dest} {exclude_str}")

  res <- run_remote(command, remote = "", verbose = verbose)

  if (res$stderr) {
    stop(glue("rsync failed: {res$stderr}"))
  }

  if (verbose) print(paste("Elapsed:", res$elapsed_time, "sec"))
}

#' Checks if a local or remote file exists.
#'
#' A wrapper around a bash script. Works with local files too if \code{remote=""}.
#'
#' @param file File path.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @return \code{TRUE} or \code{FALSE} indicating whether the file exists.
#'
#' @importFrom glue glue
#'
#' @export
#'
#' @examples
#' \dontrun{
#' file_exists_remote("~/myfile.csv", remote = "me@@myserver")
#' # [1] TRUE
#' }
file_exists_remote <- function(file, remote = "", verbose = FALSE) {
  if (is_remote_local(remote)) {
    file.exists(file)
  } else { # assume remote is unix based
    cmd <- glue::glue("(ls {file} >> /dev/null 2>&1 && echo TRUE) || echo FALSE")
    run_remote(cmd = cmd, remote = remote, verbose = verbose)$stdout %>% sub(".*(TRUE|FALSE)$", "\\1", .) %>% as.logical()
  }
}

#' Creates a remote directory with the specified group ownership and permissions.
#'
#' @param path Directory path. If using \code{remote}, this should be a full path or
#'             a path relative to the user's home directory.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @export
mkdir_remote <- function(path, remote = "", verbose = FALSE) {
  if (is_remote_local(remote)) {
    if (!file_exists_remote(path, remote)) {
      dir.create(path = path, recursive = TRUE, showWarnings = verbose)
    }
  } else {
    if (!file_exists_remote(path, remote)) {
      run_remote(paste("mkdir -p", path), remote = remote, verbose = verbose)
    }
  }
}

#' Read from a file remotely
#'
#' @param path Path of the file.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @importFrom readr read_file
#'
#' @export
cat_remote <- function(path, remote = NULL, verbose = FALSE) {
  if (is_remote_local(remote)) {
    readr::read_file(path)
  } else {
    run_remote(paste0("cat \"", path, "\""), remote = remote, verbose = verbose)$stdout
  }
}

#' Write to a file remotely
#'
#' @param x The text to write to the file.
#' @param path Path of the file.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @importFrom readr write_lines
#'
#' @export
write_remote <- function(x, path, remote, verbose = FALSE) {
  if (is_remote_local(remote)) {
    readr::write_lines(x, path)
  } else {
    tmpfile <- tempfile()
    readr::write_lines(x, tmpfile)

    if (!is(remote, "ssh_session")) {
      remote <- ssh::ssh_connect(remote)
      on.exit(ssh::ssh_disconnect(remote))
    }

    cp_remote(
      remote_src = "",
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
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @export
ls_remote <- function(path, remote = NULL, verbose = FALSE) {
  if (is_remote_local(remote)) {
    list.files(path)
  } else {
    run_remote(paste0("ls -1 \"", path, "\""), remote = remote, verbose = verbose)$stdout
  }
}

#' Show the status of Grid Engine jobs and queues
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @export
qstat_remote <- function(remote = NULL, verbose = FALSE) {
  run_remote("qstat", remote = remote, verbose = verbose)$stdout
}

#' Tests whether the remote is a local host or not.
#'
#' @param remote A putative remote machine. This function will return true if \code{remote} is \code{NULL}, \code{NA}, or \code{""}.
is_remote_local <- function(remote) {
  !is(remote, "ssh_session") && ((is.character(remote) && nchar(remote) == 0) || is.null(remote) || is.na(remote))
}
