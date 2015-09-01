create.qsub.object <- function(remote, qsub.src.dir, qsub.remote.dir, qsub.tmp.foldername) {
  # folder paths
  src.dir <- paste0(qsub.src.dir, "/", qsub.tmp.foldername)
  remote.dir <- paste0(qsub.remote.dir, "/", qsub.tmp.foldername)
  src.outdir <- paste0(src.dir, "/out")
  remote.outdir <- paste0(remote.dir, "/out")
  src.logdir <- paste0(src.dir, "/log")
  remote.logdir <- paste0(remote.dir, "/log")
  
  # file paths
  src.rdata1 <- paste0(src.dir, "/data1.RData")
  src.rdata2 <- paste0(src.dir, "/data2.RData")
  src.rfile <- paste0(src.dir, "/script.R")
  src.shfile <- paste0(src.dir, "/script.sh")
  remote.rdata1 <- paste0(remote.dir, "/data1.RData")
  remote.rdata2 <- paste0(remote.dir, "/data2.RData")
  remote.rfile <- paste0(remote.dir, "/script.R")
  remote.shfile <- paste0(remote.dir, "/script.sh")
  
  qsub <- list(
    remote=remote,
    src.dir=src.dir, src.outdir=src.outdir, src.logdir=src.logdir,
    remote.dir=remote.dir, remote.outdir=remote.outdir, remote.logdir=remote.logdir,
    src.rdata1=src.rdata1, src.rdata2=src.rdata2, src.rfile=src.rfile, src.shfile=src.shfile,
    remote.rdata1=remote.rdata1, remote.rdata2=remote.rdata2, remote.rfile=remote.rfile, remote.shfile=remote.shfile
  )
  class(qsub) <- "qsub"
  qsub
}

is.job.running <- function(qsub.object) {
  if (!is.null(qsub.object$job.id)) {
    qstat.out <- run.remote("qstat", qsub.object$remote)$cmd.out
    any(grepl(paste0("^ *", qsub.object$job.id, " "), qstat.out))
  } else {
    F
  }
}


#' Apply a Function over a List or Vector on PRISM!
#'
#' @param X a vector (atomic or list) or an expression object. Other objects (including classed objects) will be coerced by base::as.list.
#' @param FUN the function to be applied to each element of X.
#' @param qsub.remote ssh alias for PRISM
#' @param qsub.memory memory to allocate for each task.
#' @param qsub.src.dir a local temporary folder.
#' @param qsub.remote.dir a remote temporary folder.
#' @param qsub.tmp.foldername a random tmp folder name. 
#' @param qsub.remove.tmpdirs remove the temporary folders at the end of this procedure?
#' 
#' @import random
#' @export
#' @examples 
#' \dontrun{
#' # X=seq_len(100)
#' # FUN=function(i) { Sys.sleep(1); i }
#' # qsublapply(X, FUN, qsub.remote.dir = "/scratch/irc/personal/robrechtc/tmp")
#' }
qsublapply <- function(X, FUN, 
                       qsub.remote="prism",
                       qsub.memory="1G",
                       qsub.src.dir="/tmp", 
                       qsub.remote.dir="/scratch/irc/personal/robrechtc/tmp", 
                       qsub.tmp.foldername=paste0("qsubapply-", random::randomStrings(n=1, len=10)[1,]),
                       qsub.remove.tmpdirs=T) {
  qsub <- create.qsub.object(qsub.remote, qsub.src.dir, qsub.remote.dir, qsub.tmp.foldername)

  # check folders existance
  if (file.exists.remote(qsub$src.dir, remote="")) {
    stop("The local temporary folder already exists!")
  }
  if (file.exists.remote(qsub$remote.dir, remote=qsub$remote)) {
    stop("The remote temporary folder already exists!")
  }
  
  # create folders
  mkdir.remote(qsub$src.dir, "")
  mkdir.remote(qsub$remote.dir, qsub$remote)
  mkdir.remote(qsub$remote.outdir, qsub$remote)
  mkdir.remote(qsub$remote.logdir, qsub$remote)
  
  # save environment to RData and copy to remote
  save(list=ls(environment()), file=qsub$src.rdata1, envir=environment())
  save.image(qsub$src.rdata2)
  cp.remote(remote.src="", path.src=qsub$src.rdata1, remote.dest=qsub$remote, path.dest=qsub$remote.rdata1)
  cp.remote(remote.src="", path.src=qsub$src.rdata2, remote.dest=qsub$remote, path.dest=qsub$remote.rdata2)
  
  # save R script and copy to remote
  rscript <- paste0(
"setwd(\"", qsub$remote.dir, "\")
load(\"data1.RData\")
load(\"data2.RData\")
index <- as.integer(commandArgs(trailingOnly=T)[[1]])
out <- FUN(X[[index]])
save(out, file=paste0(\"out/out_\", index, \".RData\", sep=\"\"))")
  write(rscript, qsub$src.rfile)
  cp.remote(remote.src="", path.src=qsub$src.rfile, remote.dest=qsub$remote, path.dest=qsub$remote.rfile)
  
  # save bash script and copy to remote
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
  write(script, qsub$src.shfile)
  cp.remote(remote.src="", path.src=qsub$src.shfile, remote.dest=qsub$remote, path.dest=qsub$remote.shfile)
  
  # start job remotely and read job.id
  command.out <- run.remote(paste0("cd ", qsub$remote.dir, "; qsub script.sh"), qsub$remote)
  
  # retrieve job id
  qsub$job.id <- gsub("Your job-array ([0-9]*).*", "\\1", command.out)
  
  # while is running
  while (is.job.running(qsub)) {
    Sys.sleep(1)
  }
  
  # copy results to local
  cp.remote(remote.src=qsub$remote, path.src=qsub$remote.outdir, remote.dest="", path.dest=qsub$src.dir, recursively = T)
  
  # read RData files
  outs <- lapply(seq_along(X), function(i) {
    out <- NULL # satisfying r check
    load(paste0(qsub$src.dir, "/out/out_", i, ".RData"))
    out
  })
  
  # remove temporary folders afterwards
  if (qsub.remove.tmpdirs) {
    run.remote(paste0("rm -rf \"", qsub$remote.dir, "\""), remote=qsub$remote)
    run.remote(paste0("rm -rf \"", qsub$src.dir, "\""), remote="")
  }
  
  outs
}