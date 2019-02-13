#' @importFrom utils head
fetch_hostname_from_config <- function(host) {
  hostname <- NULL
  username <- NULL
  port <- NULL

  ssh_config <- read_ssh_config()

  if (!is.null(ssh_config)) {
    hostname_match <- grep(paste0("^ *Host ", host, " *$"), ssh_config)
    if (length(hostname_match) == 1) {
      end <- grep("^ *Host .*$", ssh_config) %>% keep(~ . > hostname_match) %>% utils::head(1)
      if (length(end) == 0) {
        end <- length(ssh_config) + 1
      }

      rel_config <- ssh_config[seq(hostname_match + 1, end - 1)]

      rel_hostname <- rel_config %>% keep(~ str_detect(., "^ *HostName "))
      if (length(rel_hostname) == 1) {
        hostname <- rel_hostname %>% str_replace_all("^ *HostName *([^ ]*).*$", "\\1")
      }

      rel_username <- rel_config %>% keep(~ str_detect(., "^ *User "))
      if (length(rel_hostname) == 1) {
        username <- rel_username %>% str_replace_all("^ *User *([^ ]*).*$", "\\1")
      }

      rel_port <- rel_config %>% keep(~ str_detect(., "^ *Port "))
      if (length(rel_hostname) == 1) {
        port <- rel_port %>% str_replace_all("^ *Port *([^ ]*).*$", "\\1")
      }
    }
  }

  lst(hostname, username, port)
}
