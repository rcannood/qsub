my.cp.remote <- function (remote.src, path.src, remote.dest, path.dest, verbose = FALSE, 
                          via.local = FALSE, local.temp.dir = tempdir(), recursively=F) {
  if (remote.src == "" || is.na(remote.src)) 
    remote.src = NULL
  path.src <- paste(c(remote.src, path.src), collapse = ":")
  if (remote.dest == "" || is.na(remote.dest)) 
    remote.dest = NULL
  path.dest <- paste(c(remote.dest, path.dest), collapse = ":")
  if (via.local) {
    path.tmp <- tempfile(pattern = "tmp_scp_", tmpdir = local.temp.dir)
    res <- run.remote(paste("scp", path.src, path.tmp), remote = "")
    if (res$cmd.error) {
      stop(paste("scp failed:", res$warn.msg))
    }
    if (verbose) 
      print(paste("Elapsed:", attr(res$cmd.out, "elapsed.time"), 
                  "sec"))
    path.src <- path.tmp
  }
  if (recursively) {
    res <- ssh.utils::run.remote(paste("scp -r", path.src, path.dest), remote = "")
  } else {
    res <- ssh.utils::run.remote(paste("scp", path.src, path.dest), remote = "")
  }
  if (res$cmd.error) {
    stop(paste("scp failed:", res$warn.msg))
  }
  if (verbose) 
    print(paste("Elapsed:", attr(res$cmd.out, "elapsed.time"), 
                "sec"))
  if (via.local) 
    ssh.utils::run.remote(paste("rm -f", path.tmp), remote = "")
}

#' Apply a Function over a List or Vector on PRISM!
#'
#' @param X a vector (atomic or list) or an expression object. Other objects (including classed objects) will be coerced by base::as.list.
#' @param FUN the function to be applied to each element of X.
#' @param qsub.memory memory to allocate for each task.
#' @param qsub.src.dir a local temporary folder.
#' @param qsub.remote.dir a remote temporary folder.
#' @param qsub.tmp.foldername a random tmp folder name. 
#' @param qsub.remove.tmpdirs remove the temporary folders at the end of this procedure?
#' 
#' @import ssh.utils
#' @import random
#' @export
#' @examples 
#' # not run 
#' # X=seq_len(100)
#' # FUN=function(i) { Sys.sleep(1); i }
#' # qsublapply(X, FUN, qsub.remote.dir = "/scratch/irc/personal/robrechtc/tmp")
qsublapply <- function(X, FUN, 
                       qsub.memory="1G",
                       qsub.src.dir="/tmp", 
                       qsub.remote.dir="/scratch/irc/personal/robrechtc/tmp", 
                       qsub.tmp.foldername=paste0("qsubapply-", random::randomStrings(n=1, len=10)[1,]),
                       qsub.remove.tmpdirs=T) {
  src.dir=paste0(qsub.src.dir, "/", qsub.tmp.foldername)
  remote.dir=paste0(qsub.remote.dir, "/", qsub.tmp.foldername)
  if (dir.exists(src.dir)) {
    stop("The local temporary folder already exists!")
  }
  if (ssh.utils::file.exists.remote(remote.dir)) {
    stop("The remote temporary folder already exists!")
  }
  
  dir.create(src.dir)
  ssh.utils::mkdir.remote(remote.dir, remote="irccluster")
  ssh.utils::mkdir.remote(paste0(remote.dir, "/log"), remote="irccluster")
  
  remote.outdir <- paste0(remote.dir, "/out")
  ssh.utils::mkdir.remote(remote.outdir, remote="irccluster")
  
  src.rdata <- paste0(src.dir, "/data.RData")
  remote.rdata <- paste0(remote.dir, "/data.RData")
  
  src.rfile <- paste0(src.dir, "/script.R")
  remote.rfile <- paste0(remote.dir, "/script.R")
  
  src.shfile <- paste0(src.dir, "/script.sh")
  remote.shfile <- paste0(remote.dir, "/script.sh")
  
  save(list=ls(environment()), file=src.rdata, envir=environment())
  #save.image(src.rdata)
  
  ssh.utils::cp.remote(remote.src="", path.src=src.rdata, remote.dest="irccluster", path.dest=remote.rdata)
  
  rscript <- paste0(
"setwd(\"", remote.dir, "\")
load(\"data.RData\")
index <- as.integer(commandArgs(trailingOnly=T)[[1]])
out <- FUN(X[[index]])
save(out, file=paste0(\"out/out_\", index, \".RData\", sep=\"\"))")
  write(rscript, src.rfile)
  ssh.utils::cp.remote(remote.src="", path.src=src.rfile, remote.dest="irccluster", path.dest=remote.rfile)
  
  script <- paste0(
"#!/bin/bash
#$ -t ", 1, "-", length(X), "
#$ -N interactive
#$ -e log/$JOB_NAME.$JOB_ID.$TASK_ID.e.txt
#$ -o log/$JOB_NAME.$JOB_ID.$TASK_ID.o.txt
#$ -l h_vmem=", qsub.memory, "
module load R
module unload gcc
export LD_LIBRARY_PATH=\"/software/shared/apps/x86_64/gcc/4.8.0/lib/:/software/shared/apps/x86_64/gcc/4.8.0/lib64:$LD_LIBRARY_PATH\"
Rscript script.R $SGE_TASK_ID")
  write(script, src.shfile)
  ssh.utils::cp.remote(remote.src="", path.src=src.shfile, remote.dest="irccluster", path.dest=remote.shfile)
  
  command <- paste0(
"ssh irccluster -T << HERE
module load gridengine
module load R
cd ", remote.dir, "
qsub script.sh
HERE")
  command.out <- system(command, intern = T)
  
  id <- gsub("Your job-array ([0-9]*).*", "\\1", command.out)
  
  check.command <- "echo qstat | ssh irccluster -T"
  while (any(grepl(paste0("^ *", id, " "), system(check.command, intern = T)))) {
    Sys.sleep(1)
  }
  
  my.cp.remote(remote.src="irccluster", path.src=remote.outdir, remote.dest="", path.dest=src.dir, recursively = T)
  
  outs <- lapply(seq_along(X), function(i) {
    out <- NULL # satisfying r check
    load(paste0(src.dir, "/out/out_", i, ".RData"))
    out
  })
  
  if (qsub.remove.tmpdirs) {
    system(paste0("rm -rf \"", src.dir, "\""))
    ssh.utils::run.remote(paste0("rm -rf \"", remote.dir, "\""), remote="irccluster")
  }
  
  outs
}