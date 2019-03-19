#' Extract values based on temporal matching finding closest Image in an ImageCollections through Google Earth Engine
#'
#' @param x Locations (sf or sp object) used for extraction
#' @param collection_name Name of ImageCollection
#' @param date_col Name of date column (Required format: yyyy-mm-dd) used for matching; Default is 'date'
#' @param reducer_name Name of reducer
#' @param reducer_scale Scale of reducer in meters
#' @param chunksize Size of chunks in which data is split to allow extracting values for large input datasets
#' @param n_cores Number of cores to use for sending extraction calls in parallel; Default (NULL) indicates  no parallelization
#' @param prefilter Whether to pre-filter the ImageCollection. Useful to speed up processing.
#' @param prefilter_buffer How many days +- the target date should be included in pre-filtering?
#'
#' @return A sf/Spatial object with extracted values added
#' @export
#'

gee_match_extract <- function(x, collection_name, date_col = "date", reducer_name = "ee.Reducer.mean()", reducer_scale = 30, chunksize = 2000, n_cores = NULL, prefilter = FALSE, prefilter_buffer = 20) {
  # clear python environment

  # convert to sf if x is Spatial* object
  is_spatial <- is(x, "Spatial")

  if(is_spatial) {
    x <- sf::st_as_sf(x)
  }

  # convert to EPSG 4326 if necessary
  crs_in <- sf::st_crs(x)

  if(crs_in$epsg != 4326){
    x <- st_transform(x, 4326)
  }

  # rename date column if necessary
  if(date_col != "date") {
    x <- dplyr::rename(x, date = date_col)
  }

  # split input into chunks (to allow extraction for large inputs)
  x_split <- split(x, rep(1:ceiling(nrow(x)/chunksize), each=chunksize, length.out=nrow(x)))

  if(is.null(n_cores)) {
    ext_list <- purrr::map(x_split, function(xi){
      fc_filepath <- file.path(tempdir(), "fc.geojson")
      sf::write_sf(xi, fc_filepath, delete_dsn = TRUE)

      # define function in python environment
      script_path <- paste(system.file(package="earthengine"), "gee_match_extract.py", sep="/")
      reticulate::source_python(script_path)

      # define image and reducer in python environment
      reticulate::py_run_string(paste0("collection =", collection_name))
      reticulate::py_run_string(paste0("reducer =", reducer_name))

      ext <- gee_match_extract_py(fc_filepath, reducer_scale, prefilter, prefilter_buffer)

      # add extracted columns to input object
      ext_dat <- dplyr::select(ext, dplyr::setdiff(colnames(ext), colnames(xi)))

      out <- dplyr::bind_cols(xi, ext_dat)

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
                                 reticulate::source_python("python/gee_match_extract.py")

                                 # define image and reducer in python environment
                                 reticulate::py_run_string(paste0("collection =", collection_name))
                                 reticulate::py_run_string(paste0("reducer =", reducer_name))

                                 ext <- gee_match_extract_py(fc_filepath, reducer_scale, prefilter, prefilter_buffer)

                                 # add extracted columns to input object
                                 ext_dat <- dplyr::select(ext, dplyr::setdiff(colnames(ext), colnames(xi)))

                                 out <- dplyr::bind_cols(xi, ext_dat)

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
    out <- st_transform(out, crs_in$epsg)
  }
  out

}
