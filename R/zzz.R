.onLoad <- function(libname, pkgname) {
  ee <<- reticulate::import("ee", delay_load = TRUE)
  json <<- reticulate::import("json", delay_load = TRUE)
  pandas <<- reticulate::import("pandas", delay_load = TRUE)
}
