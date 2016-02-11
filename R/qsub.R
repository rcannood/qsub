#' Create a new server config file
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry.
#' @param src.dir A directory on the local machine in which to store temporary files.
#' @param remote.dir A directory on the remote machine in which to store temporary files.
#' @param file The default location to store the R2PRISM config file.
#'
#' @export
#'
#' @examples
#' create.server.config("myservername", "~/tmpfolderlocal", "~/tmpfolder_remote")
create.server.config <- function(remote, src.dir, remote.dir, file = "~/.local/share/R2PRISM/config") {
  df <- data.frame(remote, src.dir, remote.dir)
  dir.name <- sub("[^/]*$", "", file)
  if (!dir.exists(dir.name)) dir.create(dir.name, recursive = T)
  write.table(df, file, sep="\t", col.names = T, row.names = F)
}

#' Read a server config from file
#'
#' @param file The default location to store the R2PRISM config file.
#'
#' @return A server configuration object
#' @export
server.config.from.file <- function(file = "~/.local/share/R2PRISM/config") {
  if (file.exists(file)) {
    l <- as.list(read.table(file, sep = "\t", header = T, stringsAsFactors = F, check.names = F))
    class(l) <- "PRISM::serverconfig"
    l
  } else {
    stop("Please run ", sQuote("create.server.config"), " first or set the server.config manually with ", sQuote("manual.server.config"))
  }
}
#' Use a one-time manual server config
#'
#' @param remote Remote machine specification for ssh, in format such as \code{user@@server} that does not
#'        require interactive password entry.
#' @param src.dir A directory on the local machine in which to store temporary files.
#' @param remote.dir A directory on the remote machine in which to store temporary files.
#'
#' @return A server configuration object
#' @export
manual.server.config <- function(remote, src.dir, remote.dir) {
  l <- list(remote=remote, src.dir=src.dir, remote.dir=remote.dir)
  class(l) <- "PRISM::serverconfig"
  l
}


#' Create a qsub configuration object.
#'
#' @param name The name of the execution
#' @param num.cores The number of cores to allocate per task
#' @param memory The memory to allocate per core.
#' @param verbose Print out any ssh commands
#' @param tmp.foldername A temporary name for this execution.
#' @param wait If \code{TRUE}, will wait until the execution has finished by periodically checking the job status.
#' @param remove.tmpdirs If \code{TRUE}, will remove everything that was created related to this execution at the end.
#' @param r.module The R module to use
#' @param stop.on.error If \code{TRUE}, will stop when an error occurs, else returns a NA for errored instances.
#' @param max.tasks The maximum number of tasks to spawn
#' @param server.config A server configuration file
#'
#' @import random
#' @export
qsub.configuration <- function(
  r.module = "R", name = "R2PRISM", num.cores = 1, memory = "4G", verbose = F,
  tmp.foldername = paste0(name, "-", random::randomStrings(n = 1, len = 10)[1,]),
  wait = T, remove.tmpdirs = T, stop.on.error = T, max.tasks = NULL,
  server.config = server.config.from.file()
) {
  if (!class(server.config) == "PRISM::serverconfig") {
    stop(sQuote("server.config"), " must be created from ", sQuote("manual.server.config"), or, sQuote("create.server.config"))
  }
  remote <- server.config$remote
  src.dir <- paste0(server.config$src.dir, "/", tmp.foldername)
  remote.dir <- paste0(server.config$remote.dir, "/", tmp.foldername)

  qsub <- list(
    r.module=r.module,
    name=name,
    num.cores=num.cores,
    memory=memory,
    verbose=verbose,
    remote=remote,
    wait=wait,
    max.tasks=max.tasks,
    remove.tmpdirs=remove.tmpdirs,
    stop.on.error=stop.on.error,
    src.dir=src.dir,
    remote.dir=remote.dir,
    src.outdir=paste0(src.dir, "/out"),
    remote.outdir=paste0(remote.dir, "/out"),
    src.logdir=paste0(src.dir, "/log"),
    remote.logdir=paste0(remote.dir, "/log"),
    src.rdata1=paste0(src.dir, "/data1.RData"),
    src.rdata2=paste0(src.dir, "/data2.RData"),
    remote.rdata1=paste0(remote.dir, "/data1.RData"),
    remote.rdata2=paste0(remote.dir, "/data2.RData"),
    remote.rfile=paste0(remote.dir, "/script.R"),
    remote.shfile=paste0(remote.dir, "/script.sh")
  )

  class(qsub) <- "qsub_configuration"
  qsub
}

setup.execution <- function(qsub.config, environment1, environment2, rcode) {
  # check folders existance
  if (file.exists.remote(qsub.config$src.dir, remote="", verbose=qsub.config$verbose)) {
    stop("The local temporary folder already exists!")
  }
  if (file.exists.remote(qsub.config$remote.dir, remote=qsub.config$remote, verbose=qsub.config$verbose)) {
    stop("The remote temporary folder already exists!")
  }

  # create folders
  mkdir.remote(qsub.config$src.dir, "", verbose=qsub.config$verbose)
  mkdir.remote(qsub.config$remote.dir, remote=qsub.config$remote, verbose=qsub.config$verbose)
  mkdir.remote(qsub.config$remote.outdir, remote=qsub.config$remote, verbose=qsub.config$verbose)
  mkdir.remote(qsub.config$remote.logdir, remote=qsub.config$remote, verbose=qsub.config$verbose)

  # save environment to RData and copy to remote
  save(list=names(environment1), file=qsub.config$src.rdata1, envir=environment1)
  save(list=names(environment2), file=qsub.config$src.rdata2, envir=environment2)
  cp.remote(remote.src="", path.src=qsub.config$src.rdata1, remote.dest=qsub.config$remote, path.dest=qsub.config$remote.rdata1, verbose=qsub.config$verbose)
  cp.remote(remote.src="", path.src=qsub.config$src.rdata2, remote.dest=qsub.config$remote, path.dest=qsub.config$remote.rdata2, verbose=qsub.config$verbose)

  # save R script and copy to remote
  rscript <- paste0(
    "setwd(\"", qsub.config$remote.dir, "\")\n",
    "load(\"data1.RData\")\n",
    "load(\"data2.RData\")\n",
    "index <- as.integer(commandArgs(trailingOnly=T)[[1]])\n",
    rcode, "\n",
    "save(out, file=paste0(\"out/out_\", index, \".RData\", sep=\"\"))\n"
  )
  write.remote(rscript, qsub.config$remote.rfile, remote=qsub.config$remote, verbose=qsub.config$verbose)

  # save bash script and copy to remote
  shscript <- with(qsub.config, paste0(
    "#!/bin/bash\n",
    ifelse(num.cores==1, "", paste0("#$ -pe serial ", num.cores, "\n")),
    "#$ -t 1-", num.tasks, "\n",
    "#$ -N ", name, "\n",
    "#$ -e log/$JOB_NAME.$JOB_ID.$TASK_ID.e.txt\n",
    "#$ -o log/$JOB_NAME.$JOB_ID.$TASK_ID.o.txt\n",
    ifelse(is.null(max.tasks) || !is.integer(max.tasks) || !is.finite(max.tasks) || x != round(max.tasks), "", paste0("#$ -tc ", max.tasks, "\n")),
    "#$ -l h_vmem=", memory, "\n",
    "cd ", qsub.config$remote.dir, "\n",
    "module unload R\n",
    "module unload gcc\n",
    "module load ", r.module, "\n",
    "export LD_LIBRARY_PATH=\"/software/shared/apps/x86_64/gcc/4.8.0/lib/:/software/shared/apps/x86_64/gcc/4.8.0/lib64:$LD_LIBRARY_PATH\"\n",
    "Rscript script.R $SGE_TASK_ID\n"
  ))
  write.remote(shscript, qsub.config$remote.shfile, remote=qsub.config$remote, verbose=qsub.config$verbose)

  NULL
}

execute.job <- function(qsub.config) {
  # start job remotely and read job.id
  submit.command <- paste0("cd ", qsub.config$remote.dir, "; qsub script.sh")
  output <- run.remote(submit.command, remote=qsub.config$remote, verbose=qsub.config$verbose)

  # retrieve job id
  job.id <- gsub("Your job-array ([0-9]*).*", "\\1", output$cmd.out)

  as.character(job.id)
}

#' Apply a Function over a List or Vector on PRISM!
#'
#' @param X A vector (atomic or list) or an expression object. Other objects (including classed objects) will be coerced by base::as.list.
#' @param FUN The function to be applied to each element of X.
#' @param qsub.config The configuration to use for this execution.
#' @export
#' @examples
#' \dontrun{
#' qsub.lapply(
#'   X=seq_len(3),
#'   FUN=function(i) { Sys.sleep(1); i+1 },
#'   qsub.config=qsub.configuration(
#'     remote.dir="/scratch/irc/personal/robrechtc/tmp")
#' )
#'
#' promise <- qsub.lapply(
#'   X=seq_len(3),
#'   FUN=function(i) { Sys.sleep(1); i+1 },
#'   qsub.config=qsub.configuration(
#'     remote.dir="/scratch/irc/personal/robrechtc/tmp",
#'     wait=F)
#' )
#' qsub.retrieve(promise)
#' }
qsub.lapply <- function(X, FUN, qsub.config=qsub.configuration()) {
  qsub.config$num.tasks <- length(X)
  rcode <- "out <- FUN(X[[index]])\n"

  setup.execution(qsub.config, environment(), .GlobalEnv, rcode)

  qsub.config$job.id <- execute.job(qsub.config)

  if (qsub.config$wait) {
    qsub.retrieve(qsub.config)
  } else {
    qsub.config
  }
}

#' Calculate the results of a function on PRISM!
#'
#' @param FUN the function to be executed.
#' @param qsub.config The configuration to use for this execution.
#' @export
qsub.run <- function(FUN, qsub.config=qsub.configuration()) {
  qsub.config$num.tasks <- 1
  rcode <- "out <- FUN()\n"

  setup.execution(qsub.config, environment(), .GlobalEnv, rcode)

  qsub.config$job.id <- execute.job(qsub.config)

  if (qsub.config$wait) {
    qsub.retrieve(qsub.config)
  } else {
    qsub.config
  }
}

#' Check whether a job is running.
#'
#' @param qsub.config The qsub configuration of class \code{qsub_configuration}, as returned by any qsub execution
#'
#' @export
is.job.running <- function(qsub.config) {
  if (!is.null(qsub.config$job.id)) {
    qstat.out <- run.remote("qstat", qsub.config$remote)$cmd.out
    any(grepl(paste0("^ *", qsub.config$job.id, " "), qstat.out))
  } else {
    F
  }
}

#' Retrieve the results of a qsub execution.
#'
#' @param qsub.config The qsub configuration of class \code{qsub_configuration}, as returned by any qsub execution
#' @param wait If \code{TRUE}, wait until the execution has finished in order to return the results, else returns \code{NULL} if execution is not finished.
#'
#' @export
qsub.retrieve <- function(qsub.config, wait=T) {
  if (!wait && is.job.running(qsub.config)) {
    return(NULL)
  } else {
    while (is.job.running(qsub.config)) {
      Sys.sleep(1)
    }

    # copy results to local
    cp.remote(remote.src=qsub.config$remote, path.src=qsub.config$remote.outdir, remote.dest="", path.dest=qsub.config$src.outdir, recursively = T)

    # read RData files
    outs <- lapply(seq_len(qsub.config$num.tasks), function(i) {
      out <- NULL # satisfying r check
      file <- paste0(qsub.config$src.dir, "/out/out_", i, ".RData")
      if (file.exists(file)) {
        load(file)
        out
      } else if (qsub.config$stop.on.error) {
        stop("Could not load file \"", file, "\". Please check the error logs.")
      } else {
        warning("Could not load file \"", file, "\". Please check the error logs.")
        NA
      }
    })

    # remove temporary folders afterwards
    if (qsub.config$remove.tmpdirs) {
      run.remote(paste0("rm -rf \"", qsub.config$remote.dir, "\""), remote=qsub.config$remote)
      run.remote(paste0("rm -rf \"", qsub.config$src.dir, "\""), remote="")
    }

    return(outs)
  }
}
