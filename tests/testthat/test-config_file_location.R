context("config_file_location")

test_that("should be able to set and get a default qsub config", {
  loc <- config_file_location()

  if (file.exists(config_file_location())) {
    prev <- get_default_qsub_config()
    on.exit(set_default_qsub_config(prev))
  }

  config <- create_qsub_config(remote = "foo", local_tmp_path = "/help", remote_tmp_path = "/bar")
  set_default_qsub_config(config)

  new <- get_default_qsub_config()

  expect_equal(new, config)
})
