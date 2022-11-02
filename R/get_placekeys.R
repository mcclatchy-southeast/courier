#' Function for parsing bulk API responses
#' @keywords internal
parse_bulk_pk <- function(resp) {
  res <- content(resp)

  map_chr(res, pluck, "placekey", .default = NA_character_)
}

#' Get placekeys using the bulk API
#'
#' Fetch placekeys using the bulk API. Automatically paginates. Currently verbose.
#'
#' \cr\cr Modified version #' of
#' [Josiah Perry's placekey package](https://github.com/JosiahParry/placekey/blob/master/R/get_placekeys.R)
#' and bulk upload function.
#'
#' @param location_name The name of the place.
#' @param street_address The street address of the place.
#' @param city The city where the place is located.
#' @param region The second-level administrative region below nation for the place.
#' In the US, this is the state.
#' @param postal_code The postal code for the place.
#' @param iso_country_code The ISO 2-letter Country Code for the place.
#' Currently may only be US.
#' @param latitude Longitude in decimal degrees.
#' @param longitude Latitude in decimal degrees.
#' @param strict_address_match If set to `TRUE`, a Placekey is only returned if
#' all fields identify the place as being at the exact address specified. If set
#' to `FALSE`, the Placekey returned may ignore unit/apartment/floor information.
#' Optional. Default `FALSE`.
#' @param strict_name_match  If set to `TRUE`, a Placekey is only returned if all
#' fields identify the POI as having the exact name specified. Optional.
#' Default `FALSE`.
#' @param verbose Include logging to the console. Default `FALSE`.
#' @param key API key obtained from [https://www.placekey.io/](https://www.placekey.io/)
#' @param ... Unused; allows \code{purrr::pmap_chr} to call \code{get_placekey}
#' with columns not used in the Placekey call.
#'
#' @importFrom httr add_headers POST content
#' @importFrom purrr discard
#' @importFrom dplyr %>%
#' @import httr
#' @import purrr
#' @import lubridate
#'
#' @return
#' @export
#'
#' @examples
#' \dontrun{
#' #create a test dataframe
#' loc_test <- dplyr::tibble(
#'   location_name = rep(NA, 7),
#'   street_address = c('421 Fayetteville St', '2619 Western Blvd', '319 Fayetteville St',
#'                      '1205 Front Street', '2505 Atlantic Ave', '827 W Morgan St',
#'                      '126 E Cabarrus St'),
#'   city = rep('Raleigh', 7),
#'   region = rep('North Carolina', 7),
#'   postal_code = c("27601", '27606', '27601', '27609','27604', '27603', '27601'),
#'   iso_country_code = rep('US', 7)
#' ) %>%
#'   dplyr::slice(rep(1:dplyr::n(), each = 1443))
#' #test the bulk uploader
#' loc_test_pk <- loc_test %>%
#'   dplyr::mutate(placekey = get_placekeys(
#'     location_name,
#'     street_address,
#'     city,
#'     region,
#'     postal_code,
#'     iso_country_code)
#'   )
#'   }
get_placekeys <- function(
    location_name = NA,
    street_address = NA,
    city = NA,
    region = NA,
    postal_code = NA,
    iso_country_code = NA,
    latitude = NA,
    longitude = NA,
    strict_address_match = FALSE,
    strict_name_match = FALSE,
    verbose = FALSE,
    key = Sys.getenv("PLACEKEY_SECRET"),
    ...
) {

  #set options list
  options_list <- list(
    strict_name_match = strict_name_match,
    strict_address_match = strict_address_match
  )

  # Chunking input for bulk API limits --------------------------------------

  query_id <- as.character(1:length(street_address))
  # only 100 queries can be posted at one time
  n <- 100

  # figure out how many batches need to be made
  n_chunks <- ceiling(length(street_address)/ n)
  if(verbose){
    cat('>>> QUERY NEEDS', n_chunks, 'BATCHES\n')
  }

  # find the indexes of the chunks
  chunk_starts <- seq(1, n*n_chunks, by = n)
  chunk_end <- n * (1:n_chunks)

  # modify last element of the vector so there are not extra NAs
  # in the last chunk. The last chunk never is evenly split up.
  chunk_end[length(chunk_end)] <- length(street_address)


  bulk_queries <- map2(chunk_starts, chunk_end, .f = ~{
    list(
      query_id = query_id[.x:.y],
      location_name = location_name[.x:.y],
      street_address = street_address[.x:.y],
      city = city[.x:.y],
      region = region[.x:.y],
      postal_code = postal_code[.x:.y],
      iso_country_code = iso_country_code[.x:.y],
      latitude = latitude[.x:.y],
      longitude = longitude[.x:.y]) %>%
      # using purrr::transpose to get to nested json compatible list format
      # each sub-element is a named list with each query field named
      transpose()
  })


  # Batch sending w/ rate limiting ------------------------------------------

  # start counting. only 100 bulk requests a min.
  start <- Sys.time()
  if(verbose){
    cat('>>> STARTING QUERY AT', as.character(start), '\n')
  }

  resp <- imap(bulk_queries, .f = ~{

    # discard any NA values so that lat & long are included or removed as needed
    queries <- map(.x, discard, is.na)
    if(verbose){
      cat('>>> GETTING BATCH', .y, 'AT',  as.character(Sys.time()), '\n')
    }

    # if we reach the 101st we sleep the remainder of the minute (if any is left)
    if ((.y > 1) & (.y %% 100 == 1)) {

      #integer division with seconds only
      remainder <- (.y %/% 100) * 60 - (as.numeric(Sys.time()) - as.numeric(start))

      if (remainder > 0) {
        if(verbose){
          cat('>>> SLEEPING FOR', remainder, 'SECONDS AT', as.character(Sys.time()),'\n')
        }

        Sys.sleep(remainder)

        if(verbose){
          cat('>>> RESTARTING AT', as.character(Sys.time()), '\n')
        }
      }
    }

    # using retry thrice just in case
    query <- httr::RETRY("POST",
                         url = "https://api.placekey.io/v1/placekeys",
                         body = list(queries = queries, options = options_list),
                         httr::add_headers(apikey = key), encode = "json",
                         times = 3)

    # parse the bulk placekey response
    parse_bulk_pk(query)
  })

  # make into character vector
  unlist(resp)

  total_time <- lubridate::seconds_to_period(as.numeric(Sys.time()) - as.numeric(start))

  if(verbose){
    cat('>>> QUERIES FINISHED AT', as.character(Sys.time()), 'AFTER', as.character(total_time), '\n' )
  }

}
