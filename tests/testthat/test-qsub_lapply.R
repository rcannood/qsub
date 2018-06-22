context("Testing qsub_lapply")

if (Sys.getenv("PRISM_HOST") != "") {
  host <- Sys.getenv("PRISM_HOST")
  remote_tmp_path <- Sys.getenv("PRISM_REMOTEPATH")
  privkey <- Sys.getenv("PRISM_PRIVKEY")
  pubkey <- Sys.getenv("PRISM_PUBKEY")

  readr::write_lines(privkey, path = "~/.ssh/id_rsa")
  readr::write_lines(pubkey, path = "~/.ssh/id_rsa.pub")

  qsub_config <- create_qsub_config(
    remote = host,
    local_tmp_path = tempfile(),
    remote_tmp_path = remote_tmp_path
  )

  out <- qsub_lapply(1:3, function(i) i + 1, qsub_config = qsub_config)

  expect_equal(out, list(1,2,3))
}
