#' Extract values of Images or ImageCollections through Google Earth Engine
#'
#' @param x Locations (sf or sp object) used for extraction
#' @param image_name Name of Image or ImageCollection
#' @param select_bands Names of the image bands to select in Image or ImageCollection
#' @param filter_date If image_name is an ImageCollection, character vector with dates; minimum date will be used as start, maximum date as end argumnet for filtering
#' @param reducer_name Name of reducer
#' @param reducer_scale Scale of reducer in meters
#' @param chunksize Size of chunks in which data is split to allow extracting values for large input datasets
#' @param n_cores Number of cores to use for sending extraction calls in parallel; Default (NULL) indicates  no parallelization
#'
#' @return A sf/Spatial object with extracted values added
#' @export
#'

gee_extract <- function(x, image_name, select_bands = NULL, filter_date = NULL, reducer_name = "ee.Reducer.mean()", reducer_scale = 30, chunksize = 2000, n_cores = NULL) {
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
      script_path <- paste(system.file(package="geextractr"), "gee_extract.py", sep="/")
      reticulate::source_python(script_path)

      # define image and reducer in python environment
      reticulate::py_run_string(paste0("gee_object =", image_name))
      reticulate::py_run_string(paste0("reducer =", reducer_name))

      # select image bands if provided
      if(!is.null(select_bands)){
        select_string <- paste(shQuote(select_bands), collapse = ",")
        reticulate::py_run_string(paste0("gee_object = gee_object.select(", select_string, ")"))
      }

      # check whether image_name is and ImageCollection
      is_ic <- grepl("ImageCollection", image_name)

      # apply temporal filter with .filterDate() if provided (only valid for ImageCollections)
      if(!is.null(filter_date) & is_ic){
        min_date <- min(filter_date, na.rm=TRUE)
        max_date <- max(filter_date, na.rm=TRUE)
        select_dates <- c(min_date, max_date)
        select_string <- paste(shQuote(select_dates), collapse = ",")

        reticulate::py_run_string(paste0("gee_object = gee_object.filterDate(", select_string, ")"))

      }

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
                                   script_path <- paste(system.file(package="geextractr"), "gee_extract.py", sep="/")
                                   reticulate::source_python(script_path)

                                   # define image and reducer in python environment
                                   reticulate::py_run_string(paste0("gee_object =", image_name))
                                   reticulate::py_run_string(paste0("reducer =", reducer_name))

                                   # select image bands if provided
                                   if(!is.null(select_bands)){
                                     select_string <- paste(shQuote(select_bands), collapse = ",")
                                     reticulate::py_run_string(paste0("gee_object = gee_object.select(", select_string, ")"))
                                   }

                                   # check whether image_name is and ImageCollection
                                   is_ic <- grepl("ImageCollection", image_name)

                                   # if image_name is an ImageCollection, apply spatial filter using .filterBounds()
                                   if(is_ic) {
                                     reticulate::py_run_string("gee_object = gee_object.filterBounds(gee_object.geometry())")
                                   }
                                   # apply temporal filter with .filterDate() if provided (only valid for ImageCollections)
                                   if(!is.null(filter_date) & is_ic){
                                     min_date <- min(filter_date, na.rm=TRUE)
                                     max_date <- max(filter_date, na.rm=TRUE)
                                     select_dates <- c(min_date, max_date)
                                     select_string <- paste(shQuote(select_dates), collapse = ",")

                                     reticulate::py_run_string(paste0("gee_object = gee_object.filterDate(", select_string, ")"))

                                   }

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
    out <- methods::as(x, "Spatial")
  }
  if(crs_in$epsg != 4326){
    out <- sf::st_transform(out, crs_in$epsg)
  }
  out

}
