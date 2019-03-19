#' Extract values of Images or ImageCollections through Google Earth Engine
#'
#' @param x Locations (sf or sp object) used for extraction
#' @param image_name Name of Image or ImageCollection
#' @param reducer_name Name of reducer
#' @param reducer_scale Scale of reducer in meters
#' @param chunksize Size of chunks in which data is split to allow extracting values for large input datasets
#' @param n_cores Number of cores to use for sending extraction calls in parallel; Default (NULL) indicates  no parallelization
#'
#' @return A sf/Spatial object with extracted values added
#' @export
#'

gee_extract <- function(x, image_name, reducer_name = "ee.Reducer.mean()", reducer_scale = 30, chunksize = 2000, n_cores = NULL) {
  # clear python environment

  # convert to sf if x is Spatial* object
  is_spatial <- methods::is(x, "Spatial")

  if(is_spatial) {
    x <- sf::st_as_sf(x)
  }

  # convert to EPSG 4326 if necessary
  crs_in <- sf::st_crs(x)

  if(crs_in$epsg != 4326){
    x <- st_transform(x, 4326)
  }

  # split input into chunks (to allow extraction for large inputs)
  x_split <- split(x, rep(1:ceiling(nrow(x)/chunksize), each=chunksize, length.out=nrow(x)))

  if(is.null(n_cores)) {
    ext_list <- purrr::map(x_split, function(xi){
      fc_filepath <- file.path(tempdir(), "fc.geojson")
      sf::write_sf(xi, fc_filepath, delete_dsn = TRUE)

      # define function in python environment
      script_path <- paste(system.file(package="earthengine"), "gee_extract.py", sep="/")
      reticulate::source_python(script_path)

      # define image and reducer in python environment
      reticulate::py_run_string(paste0("image =", image_name))
      reticulate::py_run_string(paste0("reducer =", reducer_name))

      ext <- gee_extract_py(fc_filepath, reducer_scale)

      # add extracted columns to input object
      ext_dat <- dplyr::select(ext, dplyr::setdiff(colnames(ext), colnames(xi)))

      if("image_date" %in% colnames(ext_dat)) {
        ext_split <- split(ext_dat, ext_dat$image_date)
        ext_cbind <- purrr::map(ext_split, function(ext_split_dat) dplyr::bind_cols(xi, ext_split_dat))
        out <- do.call(rbind, ext_cbind)
        row.names(out) <- NULL
      } else {
        out <- dplyr::bind_cols(xi, ext_dat)
      }

      # remove tmp files
      unlink(fc_filepath)

      out
    })
  } else {

    cl <- parallel::makeCluster(n_cores)
    doParallel::registerDoParallel(cl)

    ext_list <- foreach::foreach(xi = x_split,
                                 .packages = c("reticulate", "purrr", "dplyr", "sf")) %dopar% {

                                   fc_filepath <- file.path(tempdir(), "fc.geojson")
                                   sf::write_sf(xi, fc_filepath, delete_dsn = TRUE)

                                   # define function in python environment
                                   reticulate::source_python("python/gee_extract.py")

                                   # define image and reducer in python environment
                                   reticulate::py_run_string(paste0("image =", image_name))
                                   reticulate::py_run_string(paste0("reducer =", reducer_name))

                                   ext <- gee_extract_py(fc_filepath, reducer_scale)

                                   # add extracted columns to input object
                                   ext_dat <- dplyr::select(ext, dplyr::setdiff(colnames(ext), colnames(xi)))

                                   if("image_date" %in% colnames(ext_dat)) {
                                     ext_split <- split(ext_dat, ext_dat$image_date)
                                     ext_cbind <- purrr::map(ext_split, function(ext_split_dat) dplyr::bind_cols(xi, ext_split_dat))
                                     out <- do.call(rbind, ext_cbind)
                                     row.names(out) <- NULL
                                   } else {
                                     out <- dplyr::bind_cols(xi, ext_dat)
                                   }

                                   # remove tmp files
                                   unlink(fc_filepath)

                                   out

                                 }

    parallel::stopCluster(cl)

  }

  out <- do.call(rbind, ext_list)
  row.names(out) <- NULL

  # recover input attributes (Spatial* obejct / CRS)
  if(is_spatial) {
    out <- as(x, "Spatial")
  }
  if(crs_in$epsg != 4326){
    out <- sf::st_transform(out, crs_in$epsg)
  }
  out

}
