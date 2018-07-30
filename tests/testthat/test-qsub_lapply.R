context("Testing qsub_lapply")

skip_on_cran()

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
    out <- qsub_lapply(2:4, function(i) i + 1, qsub_config = qsub_config)
    expect_equal(out, list(3,4,5))
  })

  test_that("qsub_lapply works with characters X", {
    out <- qsub_lapply(letters, function(char) paste0("__", char, "__"), qsub_config = qsub_config)
    expect_equal(out, as.list(paste0("__", letters, "__")))
  })

  test_that("batch_tasks functionality", {
    out <- qsub_lapply(
      X = seq_len(100000),
      FUN = function(i) i + 1,
      qsub_config = override_qsub_config(qsub_config, batch_tasks = 10000)
    )

    expect_equal(out, as.list(seq_len(100000) + 1))

  })

  test_that("environment objects are passed correctly", {
    y <- 10
    out <- qsub_lapply(
      X = seq_len(4)+5,
      FUN = function(i) i + y,
      qsub_config = override_qsub_config(qsub_config),
      qsub_environment = c("y")
    )

    expect_equal(out, as.list(seq_len(4) + 15))
  })

}
