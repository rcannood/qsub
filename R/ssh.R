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

#' \code{run.withwarn} - Evaluates the expression (e.g. a function call) and returns the
#' result with additional atributes:
#' \itemize{
#' \item num.warnings - number of warnings occured during the evaluation
#' \item last.message - the last warning message
#' }
#' Otherwise, \code{run.withwarn} is similar to \code{base::supressWarnings}
#' 
#' @param expr Expression to be evaluated.
#' @name run.remote
#' @title Functions to run commands remotely via \code{ssh} and capture output.
#' @rdname run.remote
run.withwarn <- function(expr) {
  .number_of_warnings <- 0L
  .last_warning_msg <- NULL
  frame_number <- sys.nframe()
  ans <- withCallingHandlers(expr, warning = function(w) {
    assign(".number_of_warnings", .number_of_warnings + 1L, envir = sys.frame(frame_number))
    assign(".last_warning_msg", w$message, envir = sys.frame(frame_number))
    invokeRestart("muffleWarning")
  })
  attr(ans, "num.warnings") <- .number_of_warnings
  attr(ans, "last.message") <- .last_warning_msg
  ans
}

#' \code{run.remote} - Runs the command locally or remotely using ssh. 
#' 
#' In \code{run.remote} the remote commands are enclosed in wrappers that allow to capture output. 
#' By default stderr is redirected to stdout.
#' If there's a genuine error, e.g., the remote command does not exist, the output is not captured. In this case, one can
#' see the output by setting \code{intern} to \code{FALSE}. However, when the command is run but exits with non-zero code, 
#' \code{run.remote} intercepts the generated warning and saves the output. 
#' 
#' The remote command will be put inside double quotes twice, so all quotes in cmd must be escaped twice: \code{\\\"}. 
#' However, if the command is not remote, i.e., \code{remote} is \code{NULL} or empty string, quotes should be escaped 
#' only once.
#' 
#' If the command itself redirects output, the \code{stderr.redirect} flag should be set to \code{FALSE}.
#' @param cmd Command to run. If run locally, quotes should be escaped once. 
#'        If run remotely, quotes should be escaped twice.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not 
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param intern Useful for debugging purposes: if there's an error in the command, the output of the remote 
#'        command is lost. Re-running with \code{intern=FALSE} causes the output to be printed to the console.
#'        Normally, we want to capture output and return it.
#' @param stderr.redirect When TRUE appends \code{2>&1} to the command. 
#'        Generally, one should use that to capture STDERR output 
#'        with \code{intern=TRUE}, but this should be set to \code{FALSE} 
#'        if the command manages redirection on its own.
#' @param verbose When \code{TRUE} prints the command.
#' @return \code{run.remote} returns 
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
#' res <- run.remote(cmd=command, remote=remote)
#' if (res$cmd.error) {
#'    stop(paste(paste(res$cmd.out, collapse="\n"), res$warn.msg, sep="\n"))
#' }
#' # Error: ls: /abcde: No such file or directory
#' # running command 'ls /abcde  2>&1 ' had status 1
#' 
#' ## Fetching result of a command on a remote server
#' 
#' # Get the file size in bytes
#' res <- run.remote("ls -la myfile.csv | awk '{print \\$5;}'", remote = "me@@myserver")
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
#' @rdname run.remote
run.remote <- function(cmd, remote = "", intern = T, stderr.redirect = T, verbose = F) {
  redir <- ifelse(stderr.redirect, " 2>&1", "")
  if (!is.null(remote) && nchar(remote) > 0) {
    command <- paste0("ssh -T ", remote, redir, " << 'LAMBORGHINIINTHESTREETSOFLONDON'\n", cmd, "\nLAMBORGHINIINTHESTREETSOFLONDON")
  } else {
    command <- paste(cmd, redir)      
  }
  if (verbose) print(command)    
  tm <- round(system.time(cmd.out <- run.withwarn(system(command, intern=intern)))[3], 3)
  attr(cmd.out, "elapsed.time") <- tm
  if (attr(cmd.out, "num.warnings") > 0) {
    return(list(cmd.error = TRUE, cmd.out = cmd.out, warn.msg = attr(cmd.out, "last.message")))
  }   
  list(cmd.error = FALSE, cmd.out = cmd.out, warn.msg = NULL)
}

#' A wrapper around the scp shell command that handles local/remote files and allows 
#' copying between remote hosts via the local machine.
#' 
#' @param remote.src Remote machine for the source file in the format \code{user@@machine} or an empty string for local.
#' @param path.src Path of the source file.
#' @param remote.dest Remote machine for the destination file in the format \code{user@@machine} or an empty string for local.
#' @param path.dest Path for the source file; can be a directory.
#' @param verbose Prints elapsed time if TRUE
#' @param via.local Copies the file via the local machine. Useful when two remote machines can't talk to each other directly.
#' @param local.temp.dir When copying via local machine, the directory to use as scratch space. 
#' @param recursively Copy a directory recursively?
#' @name cp.remote
#' @aliases cp.remote
#' @rdname cp.remote
#' @title scp wrapper
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
#' cp.remote(remote.src = "me@@myserver", path.src = "~/myfile.csv", 
#'           remote.dest = "", path.dest = getwd(), verbose = TRUE)
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
cp.remote <- function(remote.src, path.src, remote.dest, path.dest, verbose = FALSE, 
                      via.local = FALSE, local.temp.dir = tempdir(), recursively = FALSE) {   
  if (remote.src == "" || is.na(remote.src)) remote.src = NULL;
  path.src <- paste(c(remote.src, path.src), collapse = ":")
  
  if (remote.dest == "" || is.na(remote.dest)) remote.dest = NULL;
  path.dest <- paste(c(remote.dest, path.dest), collapse = ":")
  
  scp.command <- ifelse(recursively, "scp -r", "scp")
  
  if (via.local) {
    path.tmp <- tempfile(pattern = "tmp_scp_", tmpdir = local.temp.dir) 
    res <- run.remote(paste(scp.command, path.src, path.tmp), remote="")
    if (res$cmd.error) {
      stop(paste("scp failed:", res$warn.msg))
    }
    if (verbose) print(paste("Elapsed:", attr(res$cmd.out, "elapsed.time"), "sec"))
    path.src <- path.tmp
  }
  
  res <- run.remote(paste(scp.command, path.src, path.dest), remote="")
  if (res$cmd.error) {
    stop(paste("scp failed:", res$warn.msg))
  }
  if (verbose) print(paste("Elapsed:", attr(res$cmd.out, "elapsed.time"), "sec"))   
  
  if (via.local) run.remote(paste("rm -f", path.tmp), remote="")
}


#' Checks if a local or remote file exists.
#' 
#' A wrapper around a bash script. Works with local files too if \code{remote=""}.
#' @param file File path.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not 
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @return \code{TRUE} or \code{FALSE} indicating whether the file exists.
#' @rdname file.exists.remote
#' @examples 
#' \dontrun{
#' file.exists.remote("~/myfile.csv", remote = "me@@myserver")
#' # [1] TRUE
#' }
file.exists.remote <- function(file, remote = "") {
  cmd <- paste("if [ -e ", file, " ] ; then echo TRUE; else echo FALSE; fi ", sep="")
  res <- run.remote(cmd, remote)
  if (res$cmd.error) {
    stop(paste("file.exists.remote: ERROR", res$cmd.out, res$warn.msg, sep="\n"))
  }
  as.logical(res$cmd.out)
}

#' Measure the resident memory usage of a process.
#' 
#' Returns the memory usage in KB of a process with the specified process id. By 
#' default, returns the memory usage of the current R process. This can be used
#' to measure and log the memory usage of the R process during script execution.
#' @param pid Process ID (default is the current process id).
#' @return The resident memory usage in KB. 
#' @examples
#' \dontrun{
#' mem.usage()
#' # [1] 37268
#' }
#' @rdname mem.usage
mem.usage <- function(pid = Sys.getpid()) {
  df <- read.delim(pipe("ps axo pid,rss"), sep = ""); 
  df[df$PID == pid, "RSS"] 
}

#' Creates a remote directory with the specified group ownership and permissions.
#' 
#' If the directory already exists, attempts to set the group ownership to the 
#' \code{user.group}. The allowed group permissions are one of 
#' \code{c("g+rwx", "g+rx", "go-w", "go-rwx")}, or \code{"-"}. The value 
#' \code{"-"} means "don't change permissions". 
#' @param path Directory path. If using \code{remote}, this should be a full path or 
#'             a path relative to the user's home directory.
#' @param user.group The user group. If NULL, the default group is used.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not 
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param permissions The group permissions on the directory. Default is 'rwx'.
#' @rdname mkdir.remote
#' @note This may not work on Windows.
# COMPATIBILITY WARNING: WINDOWS 
mkdir.remote <- function(path, user.group = NULL, remote = "", permissions = c("g+rwx", "g+rx", "go-w", "go-rwx", "-")) {
  permissions <- match.arg(permissions)
  if (!file.exists.remote(path, remote = remote)) {
    run.remote(paste("mkdir -p", path), remote = remote)
  }
  if (!is.null(user.group)) {
    run.remote(paste("chgrp -R", user.group, path), remote = remote)
  }
  if (permissions != "-") {
    run.remote(paste("chmod ", permissions, " ", path, sep = ""), remote = remote)         
  }
}

#' Checks for processes running on a local or remote machine.
#' 
#' One of the use cases for this function is to ensure that an R process is 
#' already running and not start another one accidentally.
#' @param grep.string  String(s) to check for in \code{ps}. If a vector, runs a chain of piped grep commands for each string.
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not 
#'        require interactive password entry. For local execution, pass an empty string "" (default).
#' @param stop.if.any  Stop if any of \code{grep.string} is running
#' @param stop.if.none Stop if none of \code{grep.string} is running
#' @param count.self When \code{FALSE}, excludes the calling process name from the count, if it gets matched. 
#' @param ps.options   Gives the ability to run different options to ps.
#' @seealso \code{run.remote}
#' @rdname ps.grep.remote
#' @note This may not work on Windows.
#' @importFrom stringr str_detect
#' @examples 
#' \dontrun{
#' # Check if Eclipse is running.
#' ps.grep.remote("Eclipse", remote = "")
#' # [1] TRUE
#' }
ps.grep.remote <- function(
  grep.string,           # string(s) to check for in 'ps'
  remote,                # remote location to look for script
  stop.if.any = FALSE,   # stop if any of 'grep.string' running
  stop.if.none = FALSE,  # stop if none of 'grep.string' running
  count.self = FALSE,    # if TRUE, will count all processes including itself; could be useful if remote is different
  ps.options = "aux"
) {
  if (length(grep.string) == 0) stop("ps.grep.remote: grep.string is empty")
  
  # Assemble command
  greps <- unlist(lapply(grep.string, function(s) { paste("egrep '", s, "'", sep = "") } ))
  pipes <- paste(greps, collapse = " | ")
  cmd <- paste("ps ", ps.options, " | ", pipes, " | grep -v 'egrep'", sep = "")
  
  # Execute ps remotely   
  res <- run.remote(cmd, remote = remote)$cmd.out
  
  # Filter again for grep.string
  res <- res[str_detect(res, grep.string)]
  
  # Check number of rows
  running <- length(res) > 0
  
  if (running && !count.self) {
    # check if what's running is the process that called this function (self)
    script.call <- paste(commandArgs(), collapse=" ")
    res.match <- sapply(res, function(s) length(grep(script.call, s, ignore.case = T, perl = T, value = T)) > 0)
    # either we didn't match (e.g. on remote host) or we matched more than one
    running <- !(sum(res.match) == 1)
  }
  
  # Stop if the 'grep.string' is running   
  if (stop.if.any && running) {
    stop(paste0("Found ", grep.string, " in ps aux on remote server '", remote, "'; stopping\n\n",
               paste(res, collapse = "\n")))
  }
  
  # Stop if the 'grep.string' is not running
  if (stop.if.none && !running) {
    stop(paste0("Did not find ", grep.string, " in ps aux on remote server '", remote, "'; stopping"))      
  }
  
  return(running)   
}

cat.remote <- function(path, remote=NULL, verbose=F) {
  run.remote(paste0("cat \"", path, "\""), remote, verbose=verbose)$cmd.out
}

ls.remote <- function(path, remote=NULL, verbose=F) {
  run.remote(paste0("ls -1 \"", path, "\""), remote, verbose=verbose)$cmd.out
}

qstat.remote <- function(remote=NULL, verbose=F) {
  run.remote("qstat", remote, verbose=verbose)$cmd.out
}
