#' Function for parsing bulk API responses
#' @keywords internal
parse_bulk_pk <- function(resp) {

  map_chr(resp, pluck, "placekey", .default = NA_character_)
}

#' Get placekeys using the bulk API
#'
#' Fetch placekeys using the bulk API. Automatically paginates.
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
#' @param key API key obtained from [https://www.placekey.io/](https://www.placekey.io/).
#' By default, loaded via environmental variable `PLACEKEY_SECRET`.
#' @param ... Unused; allows \code{purrr::pmap_chr} to call \code{get_placekey}
#' with columns not used in the Placekey call.
#'
#' @importFrom httr add_headers POST content
#' @importFrom purrr discard
#' @importFrom dplyr %>%
#' @import httr
#' @import purrr
#' @import lubridate
#' @import logger
#'
#' @return `dplyr` compatible placekeys. If address doesn't match, returns 'Invalid address'
#' @export
#'
#' @examples
#' \dontrun{
#' #create a test dataframe
#' loc_test <- dplyr::tibble(
#'   location_name = rep(NA, 7),
#'   latitude = c(35.774323243126666, 35.7798015451112, 35.77610354072599,
#'                35.822304545519394, 35.81508074465238, 35.77966604109918,
#'                35.77385533560202),
#'   longitude = c(-78.63876013558352, -78.6745428292959, -78.63886492957215,
#'                -78.6162767007042, -78.61804304210553, -78.65351432957601,
#'                -78.637435700986),
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

  #log chunk/batch size
  logger::log_info('PROCESSING ', length(street_address), ' ROWS IN ', n_chunks, ' BATCHES')

  # find the indexes of the chunks
  chunk_starts <- seq(1, n * n_chunks, by = n)
  chunk_end <- n * (1:n_chunks)

  # modify last element of the vector so there are not extra NAs
  # in the last chunk. The last chunk never is evenly split up.
  chunk_end[length(chunk_end)] <- length(street_address)


  bulk_queries <- map2(chunk_starts, chunk_end, .f = ~{

    #a note about returning errors
    query_json <- list(
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

    return(query_json)

  })

  # Batch sending w/ rate limiting ------------------------------------------

  # start counting. only 100 bulk requests a min.
  start <- Sys.time()

  resp <- imap(bulk_queries, .f = ~{

    # discard any NA values so that lat & long are included or removed as needed
    queries <- map(.x, discard, is.na)

    #log batch number
    logger::log_trace('GETTING BATCH ', .y)

    # if we reach the 101st we sleep the remainder of the minute (if any is left)
    if ((.y > 1) & (.y %% 100 == 1)) {

      #integer division with seconds only
      remainder <- (.y %/% 100) * 60 - (as.numeric(Sys.time()) - as.numeric(start))

      if (remainder > 0) {
        #log sleep time
        logger::log_debug('SLEEPING FOR ', remainder, ' SECONDS')

        Sys.sleep(remainder)

        #log restart time
        logger::log_debug('RESTARTING')
      }
    }

    #calculate 60-second interval
    last_minute <- as.numeric(Sys.time()) - 60
    #discard all values less than interval, updating globally
    assign("query_times", discard(.pkgglobalenv$query_times, ~ .x < last_minute), envir=.pkgglobalenv)
    query_count <- length(.pkgglobalenv$query_times)

    #only log if we're approaching the limit
    if(query_count > 98){
      logger::log_debug('RUNNING QUERY COUNT: ', query_count)
    }

    #evaluate size of list
    if( query_count > 100){
      sleep_time <- query_count - 100

      #log sleep time
      logger::log_debug('60 SEC RATE LIMIT EXCEEDED. SLEEPING FOR ', sleep_time, ' SECONDS')
      Sys.sleep(sleep_time)
    }

    # using retry three times, with an exception for bad request
    query <- httr::RETRY("POST",
                         url = "https://api.placekey.io/v1/placekeys",
                         body = list(queries = queries,
                                     options = options_list),
                         httr::add_headers(apikey = key),
                         encode = "json",
                         times = 5, #increase the tries now that we've weeded out bad requests
                         pause_min = 5, #increase pause time in case of server issues
                         terminate_on = c(400)
                         )

    #append time of new query to global variable list
    assign("query_times", append(.pkgglobalenv$query_times, as.numeric(Sys.time())), envir=.pkgglobalenv)

    #test for 400/bad request error code and report out
    #TODO May need to add this for all other status code
    if(query$status_code == '400'){
      error_response <- content(query)

      #check for a message-only error response in cases of nonexistent zips/lng
      if(!is.null(error_response$message)){
        error_msg <- toupper(error_response$message)
      }
      else if(!is.null(error_response$error)){
        error_msg <- toupper(error_response$error)
      }
      else{
        error_msg <- 'NO DETAILS PROVIDED'
      }

      #report error message
      logger::log_warn('BAD REQUEST, ROW ', queries[[1]]$query_id, ': ', error_msg)

      #pass in unprocessed queries
      parse_bulk_pk(queries)
    }
    else{
      # parse the bulk placekey response
      parse_bulk_pk(content(query))
    }
  })

  total_time <- lubridate::seconds_to_period(as.numeric(Sys.time()) - as.numeric(start))

  #report successful processing
  logger::log_success('BATCHES FINISHED AT ', as.character(Sys.time()), ' AFTER ', as.character(total_time))

  # make into character vector
  unlist(resp)

}
