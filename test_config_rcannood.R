library(PRISM)
exec_before <-


# Initial configuration and execution
qsub_config <- create_qsub_config(
  remote = "prism",
  local_tmp_path = "/home/rcannood/Workspace/.r2gridengine",
  remote_tmp_path = "/scratch/irc/personal/robrechtc/.r2gridengine",
  execute_before = c(
    "module unload gcc",
    "export LD_LIBRARY_PATH=\"/software/shared/apps/x86_64/gcc/4.8.0/lib/:/software/shared/apps/x86_64/gcc/4.8.0/lib64:$LD_LIBRARY_PATH\"\n"
  )
)

PRISM:::get_default_qsub_config()


qsub_lapply(
  X = seq_len(3),
  FUN = function(i) { Sys.sleep(1); i+1 },
  qsub_config = qsub_config
)

# Setting a default configuration and short hand notation for execution
set_default_qsub_config(qsub_config, permanent = T)
qsub_lapply(seq_len(3), function(i) { Sys.sleep(1); i+1 })

# Overriding a default qsub_config
qsub_lapply(seq_len(3), function(i) i + 1,
  qsub_config = override_qsub_config(name = "MyJob", wait = F))

# Retrieve results
qsub_retrieve(handle)
