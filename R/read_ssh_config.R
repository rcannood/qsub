read_ssh_config <- function() {
  if (.Platform$OS.type == "windows") {
    location <- "~/../.ssh/config"
  } else {
    location <- "~/.ssh/config"
  }

  if (file.exists(location)) {
    readr::read_lines(location)
  } else {
    NULL
  }
}
