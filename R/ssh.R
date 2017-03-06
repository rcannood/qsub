#-------------------------------------------------------------------------------
# Original implementation, package ssh.utils, by Sergei Izrailev, 2011-2014
# Several bugfixes and addition of more wrappers, package PRISM, by Robrecht Cannoodt, 2015
#-------------------------------------------------------------------------------
# Copyright 2011-2014 Collective, Inc.
# Copyright 2015 Robrecht Cannoodt
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------

#' \code{run_withwarn} - Evaluates the expression (e.g. a function call) and returns the
#' result with additional atributes:
#' \itemize{
#' \item num.warnings - number of warnings occured during the evaluation
#' \item last.message - the last warning message
#' }
#' Otherwise, \code{run_withwarn} is similar to \code{base::supressWarnings}
#'
#' @param expr Expression to be evaluated.
#' @title Functions to run commands remotely via \code{ssh} and capture output.
run_withwarn <- function(expr) {
  .number_of_warnings <- 0L
  .last_warning_msg <- NULL
  frame_number <- sys.nframe()
  ans <- withCallingHandlers(expr, warning = function(w) {
    assign(".number_of_warnings", .number_of_warnings + 1L, envir = sys.frame(frame_number))
    assign(".last_warning_msg", w$message, envir = sys.frame(frame_number))
    invokeRestart("muffleWarning")
  })
  attr(ans, "num_warnings") <- .number_of_warnings
  attr(ans, "last_message") <- .last_warning_msg
  ans
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
#' @param cmd Command to run. If run locally, quotes should be escaped once.
#'        If run remotely, quotes should be escaped twice.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param intern Useful for debugging purposes: if there's an error in the command, the output of the remote
#'        command is lost. Re-running with \code{intern=FALSE} causes the output to be printed to the console.
#'        Normally, we want to capture output and return it.
#' @param stderr_redirect When TRUE appends \code{2>&1} to the command.
#'        Generally, one should use that to capture STDERR output
#'        with \code{intern=TRUE}, but this should be set to \code{FALSE}
#'        if the command manages redirection on its own.
#' @param verbose If \code{TRUE} prints the command.
#' @return \code{run_remote} returns
#' a list containing the results of the command execution, error codes and messages.
#' \itemize{
#' \item \code{cmd.error} - flag indicating if a warning was issued because command exited with non-zero code
#' \item \code{cmd.out} - the result of the command execution. If there was no error, this contains the output
#'                        as a character array, one value per line, see \code{\link{system}}. If there was
#'                        an error (as indicated by \code{cmd.error}), this most likely contains the error message
#'                        from the command itself. The \code{elapsed.time} attribute contains the elapsed
#'                        time for the command in seconds.
#' \item \code{warn.msg} - the warning message when \code{cmd.error} is TRUE.
#' }
#' Warnings are really errors here so the error flag is set if there are warnings.
#'
#' Additionally, \code{cmd.out} has the \code{elapsed.time}, \code{num.warnings} and, if
#' the number of warnings is greater than zero, \code{last.warning} attributes.
#' @examples
#' \dontrun{
#' ## Error handling:
#' remote = ""
#' command = "ls /abcde"
#' res <- run_remote(cmd=command, remote=remote)
#' if (res$cmd.error) {
#'    stop(paste(paste(res$cmd.out, collapse="\n"), res$warn.msg, sep="\n"))
#' }
#' # Error: ls: /abcde: No such file or directory
#' # running command 'ls /abcde  2>&1 ' had status 1
#'
#' ## Fetching result of a command on a remote server
#'
#' # Get the file size in bytes
#' res <- run_remote("ls -la myfile.csv | awk '{print \\$5;}'", remote = "me@@myserver")
#' res
#' # $cmd.error
#' # [1] FALSE
#' #
#' # $cmd.out
#' # [1] "42"
#' # attr(,"num.warnings")
#' # [1] 0
#' # attr(,"elapsed.time")
#' # elapsed
#' # 1.063
#' #
#' # $warn.msg
#' # NULL
#'
#' file.length <- as.integer(res$cmd.out)
#' }
run_remote <- function(cmd, remote, intern = T, stderr_redirect = T, verbose = F) {
  redir <- ifelse(stderr_redirect, " 2>&1", "")
  if (!is.null(remote) && nchar(remote) > 0) {
    command <- paste0("ssh -T ", remote, redir, " << 'LAMBORGHINIINTHESTREETSOFLONDON'\n", cmd, "\nLAMBORGHINIINTHESTREETSOFLONDON")
  } else {
    command <- paste(cmd, redir)
  }
  if (verbose) cat("# ", gsub("\n", "\n# ", command), "\n", sep="")
  time1 <- Sys.time()
  cmd_out <- run_withwarn(system(command, intern = intern))
  time2 <- Sys.time()
  attr(cmd_out, "elapsed_time") <- as.numeric(time2 - time1, units = "secs")
  if (attr(cmd_out, "num_warnings") > 0) {
    return(list(cmd_error = TRUE, cmd_out = cmd_out, warn.msg = attr(cmd_out, "last.message")))
  }
  list(cmd_error = FALSE, cmd_out = cmd_out, warn.msg = NULL)
}

#' A wrapper around the scp shell command that handles local/remote files and allows
#' copying between remote hosts via the local machine.
#'
#' @param remote_src Remote machine for the source file in the format \code{user@@machine} or an empty string for local.
#' @param path_src Path of the source file.
#' @param remote_dest Remote machine for the destination file in the format \code{user@@machine} or an empty string for local.
#' @param path_dest Path for the source file; can be a directory.
#' @param verbose Prints elapsed time if TRUE
#' @param via_local Copies the file via the local machine. Useful when two remote machines can't talk to each other directly.
#' @param local_temp_dir When copying via local machine, the directory to use as scratch space.
#' @param recursively Copy a directory recursively?
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
cp_remote <- function(remote_src, path_src, remote_dest, path_dest, verbose = FALSE,
                      via_local = FALSE, local_temp_dir = tempdir(), recursively = FALSE) {
  if (remote_src != "" && !is.na(remote_src) && !is.null(remote_src)) {
    path_src <- paste0(remote_src, ":", path_src)
  }
  if (remote_dest != "" && !is.na(remote_dest) && !is.null(remote_dest)) {
    path_dest <- paste0(remote_dest, ":", path_dest)
  }

  scp_command <- ifelse(recursively, "scp -r", "scp")

  if (via_local) {
    path_tmp <- tempfile(pattern = "tmp_scp_", tmpdir = local_temp_dir)
    res <- run_remote(paste(scp_command, path_src, path_tmp), remote = "")
    if (res$cmd_error) {
      stop(paste("scp failed:", res$warn_msg))
    }
    if (verbose) print(paste("Elapsed:", attr(res$cmd_out, "elapsed_time"), "sec"))
    path_src <- path_tmp
  }

  res <- run_remote(paste(scp_command, path_src, path_dest), remote="")
  if (res$cmd_error) {
    stop(paste("scp failed:", res$warn_msg))
  }
  if (verbose) print(paste("Elapsed:", attr(res$cmd_out, "elapsed_time"), "sec"))

  if (via_local) run_remote(paste("rm -f", path_tmp), remote="")
}

#' A wrapper around the rsync shell command that allows copying between remote hosts via the local machine.
#'
#' @param remote_src Remote machine for the source file in the format \code{user@@machine} or an empty string for local.
#' @param path_src Path of the source file.
#' @param remote_dest Remote machine for the destination file in the format \code{user@@machine} or an empty string for local.
#' @param path_dest Path for the source file; can be a directory.
#' @param verbose Prints elapsed time if TRUE
rsync_remote <- function(remote_src, path_src, remote_dest, path_dest, verbose = F) {
  if (remote_src != "" && !is.na(remote_src) && !is.null(remote_src)) {
    path_src <- paste0("-e ssh ", remote_src, ":", path_src)
  }
  if (remote_dest != "" && !is.na(remote_dest) && !is.null(remote_dest)) {
    path_dest <- paste0("-e ssh ", remote_dest, ":", path_dest)
  }

  command <- paste0("rsync -avz ", path_src, " ", path_dest)
  res <- run_remote(command, remote = "", verbose = verbose)
  if (res$cmd_error) {
    stop(paste("rsync failed:", res$warn_msg))
  }

  if (verbose) print(paste("Elapsed:", attr(res$cmd_out, "elapsed.time"), "sec"))
}


#' Checks if a local or remote file exists.
#'
#' A wrapper around a bash script. Works with local files too if \code{remote=""}.
#' @param file File path.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#' @return \code{TRUE} or \code{FALSE} indicating whether the file exists.
#' @examples
#' \dontrun{
#' file_exists_remote("~/myfile.csv", remote = "me@@myserver")
#' # [1] TRUE
#' }
file_exists_remote <- function(file, remote = "", verbose=F) {
  secret <- "ZINNGGGGG! MESSAGE FOR YOU SIR!"
  cmd <- paste("if [ -e ", file, " ] ; then echo \"", secret, "\"; fi ", sep="")
  res <- run_remote(cmd, remote, verbose = verbose)
  if (res$cmd_error) {
    stop(paste("file_exists_remote: ERROR", res$cmd_out, res$warn_msg, sep="\n"))
  }
  any(res$cmd_out == secret)
}

#' Creates a remote directory with the specified group ownership and permissions.
#'
#' If the directory already exists, attempts to set the group ownership to the
#' \code{user_group}. The allowed group permissions are one of
#' \code{c("g+rwx", "g+rx", "go-w", "go-rwx")}, or \code{"-"}. The value
#' \code{"-"} means "don't change permissions".
#' @param path Directory path. If using \code{remote}, this should be a full path or
#'             a path relative to the user's home directory.
#' @param user_group The user group. If NULL, the default group is used.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param permissions The group permissions on the directory. Default is '-'.
#' @param verbose If \code{TRUE} prints the command.
mkdir_remote <- function(path, remote = "", user_group = NULL, permissions = "-", verbose = F) {
  if (!file_exists_remote(path, remote)) {
    run_remote(paste("mkdir -p", path), remote=remote, verbose=verbose)
  }
  if (!is.null(user_group)) {
    run_remote(paste("chgrp -R", user_group, path), remote=remote, verbose=verbose)
  }
  if (permissions != "-") {
    run_remote(paste0("chmod ", permissions, " ", path), remote=remote, verbose=verbose)
  }
}

#' Read from a file remotely
#'
#' @param path Path of the file.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
cat_remote <- function(path, remote=NULL, verbose=F) {
  run_remote(paste0("cat \"", path, "\""), remote, verbose=verbose)$cmd.out
}

#' Write to a file remotely
#'
#' @param x The text to write to the file.
#' @param path Path of the file.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
write_remote <- function(x, path, remote, verbose=F) {
  run_remote(paste0("cat > ", path, " << 'FLEMISHPLANTSFORSALE'\n", paste(x, collapse="\n"), "FLEMISHPLANTSFORSALE\n"), remote, verbose=verbose)
}

#' View the contents of a directory remotely
#'
#' @param path Path of the directory.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
ls_remote <- function(path, remote=NULL, verbose=F) {
  run_remote(paste0("ls -1 \"", path, "\""), remote, verbose=verbose)$cmd.out
}

#' Show the status of Grid Engine jobs and queues
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param verbose If \code{TRUE} prints the command.
#'
#' @export
qstat_remote <- function(remote=NULL, verbose=F) {
  run_remote("qstat", remote, verbose=verbose)$cmd.out
}

