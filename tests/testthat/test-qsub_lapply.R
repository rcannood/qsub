context("Testing qsub_lapply")

if (Sys.getenv("PRISM_HOST") != "") {
  host <- Sys.getenv("PRISM_HOST")
  remote_tmp_path <- Sys.getenv("PRISM_REMOTEPATH")

  qsub_config <- create_qsub_config(
    remote = host,
    local_tmp_path = tempfile(),
    remote_tmp_path = remote_tmp_path
  )
} else if (file.exists(config_file_location())) {
  qsub_config <- get_default_qsub_config()
} else {
  qsub_config <- NULL
}

if (!is.null(qsub_config)) {
  test_that("qsub_lapply works", {
    out <- qsub_lapply(1:3, function(i) i + 1, qsub_config = qsub_config)

    expect_equal(out, list(2,3,4))

    # test batch_tasks functionality
    out <- qsub_lapply(
      X = seq_len(100000),
      FUN = function(i) i + 1,
      qsub_config = override_qsub_config(qsub_config, batch_tasks = 10000)
    )

    expect_equal(out, as.list(seq_len(100000) + 1))


    # test whether environment objects are passed correctly
    y <- 10
    out <- qsub_lapply(
      X = seq_len(4),
      FUN = function(i) i + y,
      qsub_config = override_qsub_config(qsub_config),
      qsub_environment = c("y")
    )

    expect_equal(out, as.list(seq_len(4) + 10))
  })

}
