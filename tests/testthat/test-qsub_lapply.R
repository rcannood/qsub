context("Testing qsub_lapply")

if (Sys.getenv("PRISM_HOST") != "") {
  host <- Sys.getenv("PRISM_HOST")
  remote_tmp_path <- Sys.getenv("PRISM_REMOTEPATH")

  qsub_config <- create_qsub_config(
    remote = host,
    local_tmp_path = tempfile(),
    remote_tmp_path = remote_tmp_path
  )

  out <- qsub_lapply(1:3, function(i) i + 1, qsub_config = qsub_config)

  expect_equal(out, list(2,3,4))
}
