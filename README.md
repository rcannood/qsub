<!-- README.md is generated from README.Rmd. Please edit that file -->
qsub
====

[![Build
Status](https://travis-ci.org/rcannood/qsub.svg?branch=master)](https://travis-ci.org/rcannood/qsub)
[![AppVeyor Build
Status](https://ci.appveyor.com/api/projects/status/github/rcannood/qsub?branch=master&svg=true)](https://ci.appveyor.com/project/rcannood/qsub)
[![CRAN\_Status\_Badge](https://www.r-pkg.org/badges/version/qsub)](https://cran.r-project.org/package=qsub)
[![Coverage
Status](https://codecov.io/gh/rcannood/qsub/branch/master/graph/badge.svg)](https://codecov.io/gh/rcannood/qsub?branch=master)

qsub provides the `qsub_lapply` function, which helps you parallellise
lapply calls to gridengine clusters.

Usage
-----

After installation and configuration of the qsub package, running a job
on a cluster supporting gridengine is as easy as:

    library(qsub)
    qsub_lapply(1:3, function(i) i + 1)

    ## [[1]]
    ## [1] 2
    ## 
    ## [[2]]
    ## [1] 3
    ## 
    ## [[3]]
    ## [1] 4

Installation
------------

On unix-based systems, you will first have to install libssh.

-   deb: `apt-get install libssh-dev` (Debian, Ubuntu, etc)
-   rpm: `dnf install libssh-devel` (Fedora, EPEL) (if `dnf` is not
    install, try `yum`)
-   brew: `brew install libssh` (OSX)

You can install qsub with devtools as follows:

    devtools::install_github("rcannood/qsub")

Initial test
------------

For the remainder of this README, we will assume you have an account
called `myuser` on a cluster located at `mycluster.address.org`
listening on port `1234`. This cluster is henceforth called the
'remote'. You will require a local folder to store temporary data in
(e.g. `/tmp/r2gridengine`), and a remote folder to store temporary data
in (e.g. `/scratch/personal/myuser/r2gridengine`).

After installation of the qsub package, first try out whether you can
connect to the remote. If you have not yet set up an SSH key and
uploaded it to the remote, you will be asked for password.

    qsub_config <- create_qsub_config(
      remote = "myuser@mycluster.address.org:1234",
      local_tmp_path = "/tmp/r2gridengine",
      remote_tmp_path = "/scratch/personal/myuser/r2gridengine"
    )

    qsub_lapply(1:3, function(i) i + 1, qsub_config = qsub_config) 

Permanent configuration
-----------------------

If the previous section worked just fine, you can for a permanent
configuration of the qsub config as follows:

    set_default_qsub_config(qsub_config, permanent = TRUE)

Setting up an SSH key
---------------------

If you regularly submit jobs to the remote, it will be extremely useful
to set up an SSH key configuration. This way, you don't need to enter
your password every time you execute `qsub_lapply`.

On Windows, you will first need to install [Git
bash](https://gitforwindows.org/).

The first step will be to open bash and create a file which contains the
following content, and save it to `.ssh/config`.

    Host myhost
      HostName mycluster.address.org
      Port 1234
      User myuser

Secondly, generate an SSH key. You don't need to enter a password. If
you do, though, you will be asked for this password every time you use
your SSH key.

    ssh-keygen -t rsa

Finally, copy the public key (`id_rsa.pub`) to the remote. Never share
your private key (`id_rsa`)!

    ssh-copy-id myhost

Alternatively, if you do not have `ssh-copy-id` installed, you can run
the following:

    cat ~/.ssh/id_rsa.pub | ssh myhost "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >>  ~/.ssh/authorized_keys"

Customisation of individual runs
--------------------------------

Some tasks will require you to finetune the qsub config, for example
because they require more walltime or memory than allowed by default.
These can also be specified using the `create_qsub_config` command, or
using `override_qsub_config` if you have already created a default qsub
config. Check `?override_qsub_config` for a detailed explanation of each
of the possible parameters.

    qsub_lapply(
      X = 1:3,
      FUN = function(i) {
        # simulate a very long calculation time
        # this might annoy other users of the cluster, 
        # but sometimes there is no way around it
        Sys.sleep(sample.int(3600, 1))
        i + 1
      },
      qsub_config = override_qsub_config(
        name = "MyJob", # this name will show up in qstat
        mc_cores = 2, # the number of cores to allocate per element in X
        memory = "10G", # memory per core per task
        max_wall_time = "12:00:00" # allow each task to run for 12h
      )
    )

Asynchronous jobs
-----------------

In almost every case, it is most practical to run jobs asynchronously.
This allows you to start up a job, save the meta data, come back later,
and fetch the results from the cluster. This can be done by changing the
`wait` parameter.

    qsub_async <- qsub_lapply(
      X = 1:3,
      FUN = function(i) {
        Sys.sleep(10)
        i + 1
      },
      qsub_config = override_qsub_config(
        wait = FALSE
      )
    )

    readr::write_rds(qsub_async, "temp_file.rds")

    # you can restart your computer / R session after having saved the `qsub_async` object somewhere.
    qsub_async <- readr::read_rds("temp_file.rds")

    # if the job has finished running, this will retrieve the output
    qsub_retrieve(qsub_async)

Specify which objects gets transferred
--------------------------------------

By default, `qsub_lapply` will transfer all objects in your current
environment to the cluster. This might result in long waiting times if
the current environment is very large. You can define which objects get
transferred to the cluster as follows:

    j <- 1
    k <- rep(10, 1000000000) # 7.5 Gb
    qsub_lapply(
      X = 1:3,
      FUN = function(i) {
        i + j
      },
      qsub_environment = "j"
    )

Oh no, something went wrong
---------------------------

Inevitably, something will go break. qsub will try to help you by
reading out the log files if no output was produced.

    qsub_lapply(
      X = 1:3,
      FUN = function(i) {
        if (i == 2) stop("Something went wrong!")
        i + 1
      }
    )

    ## Error in FUN(X[[i]], ...): File: /home/rcannood/Workspace/.r2gridengine/20180625_102837_r2qsub_RJDjMlQsd7/log/log.2.e.txt
    ## by .GlobalEnv when processing object ‘’
    ## Error in (function (i)  : Something went wrong!
    ## Calls: with ... with.default -> eval -> eval -> do.call -> <Anonymous>
    ## Execution halted

Alternatively, you might anticipate possible errors but still be
interested in the rest of the output. In this case, the error will be
returned as an attribute.

    qsub_lapply(
      X = 1:3,
      FUN = function(i) {
        if (i == 2) stop("Something went wrong!")
        i + 1
      },
      qsub_config = override_qsub_config(
        stop_on_error = FALSE
      )
    )

    ## [[1]]
    ## [1] 2
    ## 
    ## [[2]]
    ## [1] NA
    ## attr(,"qsub_error")
    ## [1] "File: /home/rcannood/Workspace/.r2gridengine/20180625_102845_r2qsub_gJt73SQUkv/log/log.2.e.txt\nby .GlobalEnv when processing object ‘’\nError in (function (i)  : Something went wrong!\nCalls: with ... with.default -> eval -> eval -> do.call -> <Anonymous>\nExecution halted\n"
    ## 
    ## [[3]]
    ## [1] 4

If all help prevails, you can try to manually debug the session by not
removing the temporary files at the end of an execution by setting
`remove_tmp_folder` to `FALSE`, logging into the remote server, going to
the temporary folder located at
`get_default_qsub_config()$remote_tmp_path`, and executing `script.R`
line by line in R, by hand.

    qsub_lapply(
      X = 1:3,
      FUN = function(i) {
        if (i == 2) stop("Something went wrong!")
        i + 1
      },
      qsub_config = override_qsub_config(
        remove_tmp_folder = FALSE
      )
    )
