#set up a global environment
.pkgglobalenv <- new.env(parent = emptyenv())

#' Process address in a database
#'
#' A main worker function for fetching addresses from a database in chunks,
#' processing them with Placekey's bulk API and reporting progress via Slack.
#'
#' @param dbname Name of the database. Default is `NULL`.
#' @param host Name of the host IP address. Default is `NULL`.
#' @param user Username. Default is `NULL`.
#' @param port Port number, typically 5432. Default is `NULL`.
#' @param table_name Name of the table you want to query data
#' @param slackr_channel By default, loaded via environmental variable `SLACKR_CHANNEL`.
#' @param slackr_bot By default, loaded via environmental variable `SLACKR_BOT`.
#' @param slackr_token By default, loaded via environmental variable `SLACKR_TOKEN`.
#' @param slackr_webhook By default, loaded via environmental variable `SLACKR_WEBHOOK`.
#' @param verbose Include logging to the console. Default `FALSE`.
#'
#' @importFrom purrr discard
#' @importFrom dplyr %>%
#' @import lubridate
#' @import logger
#' @import DBI
#' @import RPostgres
#' @import slackr
#' @importFrom rlang .data
#'
#' @return
#' @export
#'
#' @examples
process_addresses <- function(dbname = NULL,
                              host = NULL,
                              user = NULL,
                              port = NULL,
                              table_name,
                              slackr_channel = Sys.getenv('SLACKR_CHANNEL'),
                              slackr_bot = Sys.getenv("SLACKR_BOT"),
                              slackr_token = Sys.getenv("SLACKR_TOKEN"),
                              slackr_webhook = Sys.getenv("SLACKR_WEBHOOK"),
                              verbose = FALSE
                              ){

  #set log level based on verbose setting
  if(verbose){
    logger::log_threshold(logger::TRACE, namespace = logger::log_namespaces())
  }
  else{
    logger::log_threshold(logger::INFO, namespace = logger::log_namespaces())
  }

  #create a log file in the current working directory
  log_path <- paste0(getwd(), '/courier_log', format(Sys.time(), '%Y%m%d%H%M'))
  file.create(log_path)
  logger::log_trace('CREATED LOG FILE AT ', log_path)

  #set logger to read out to both console and text log
  logger::log_appender(logger::appender_tee(file = log_path, append = TRUE))

  #set a primary start time
  master_start <- Sys.time()

  #utility function to keep the db connection alive
  check_connection <- function(dbname, host, port, user){
    #check if connection is valid, left item first
    if(is.null(con) || !RPostgres::dbIsValid(con)){
      #establish the connection
      con <- DBI::dbConnect(
        drv = RPostgres::Postgres(),
        dbname = dbname,
        host = host,
        port = port,
        user = user)
      return(con)
    }
    else{
      return(con)
    }
  }

  #establish variables
  con = NULL

  #authenticate to the slackbot using env variables by default
  tryCatch(
    {
      slackr::slackr_setup(channel = slackr_channel,
                           username = slackr_bot,
                           token = slackr_token,
                           incoming_webhook_url = slackr_webhook)
    },
    error = function(e){
      #log error
      logger::log_error(e$message)
    }

  )

  #assess size database, counting unkeyed addresses
  #convert to integer from integer64 format
  total_rows <- as.integer(RPostgres::dbGetQuery(check_connection(dbname, host, port, user),
                                                 paste0("SELECT count(1) FROM ", table_name, " WHERE placekey = '';"))$count)

  #calculate chunks
  #100 queries at a time, 1000 per minute
  offset = 0
  limit = 10000
  n_chunks = ceiling( total_rows / limit)

  #create empty timing list
  assign("query_times", list(), envir = .pkgglobalenv)
  #query_times <<- list()

  #start sequence counter
  sequence_count <- 1

  #report initialization
  slackr::slackr_msg(paste0('\t*Query start:* ', as.character(Sys.time()),
                           '\n*Total rows:* ', format(total_rows, big.mark = ',', scientific = FALSE),
                           '\t*Total chunks:* ', format(n_chunks, big.mark = ',', scientific = FALSE)
                           ) )

  #log the top-level readout
  logger::log_info('PROCESSING ', format(total_rows, big.mark = ',', scientific = FALSE), ' TOTAL ROWS')

  #set up a loop with a 1-indexed counter
  while(sequence_count <= n_chunks){

    #log lap start
    lap_start <- Sys.time()

    #log new query start
    logger::log_info('STARTING QUERY ', sequence_count)

    #report completed sequence progress in intervals of 10
    if( sequence_count > 1 & (sequence_count %% 10) == 1 ){
      elapsed_time <- as.character(lubridate::seconds_to_period(as.numeric(Sys.time()) - as.numeric(master_start)))
      slackr::slackr_msg(paste0('*Rows processed:* ', format(limit * (sequence_count - 1), big.mark = ',', scientific = FALSE),
                                '\n*Lap:* ', as.character(Sys.time()), '\t*Elapsed:* ', elapsed_time
                               ))
    }

    pk_response <- try(
      {
        #fetch first db chunk
        chunk <- courier::fetch_db_data(check_connection(dbname, host, port, user),
                                        table_name = table_name,
                                        limit = limit,
                                        offset = offset)

        #start timer
        pk_start <- Sys.time()

        #get placekeys for the addresses
        chunk_pk <- chunk %>%
          dplyr::mutate(placekey = courier::get_placekeys(
            .data$location_name,
            .data$street_address,
            .data$city,
            .data$region,
            .data$postal_code,
            .data$iso_country_code,
            .data$latitude,
            .data$longitude)
          )

        #write a temporary table to the database
        RPostgres::dbWriteTable(check_connection(dbname, host, port, user),
                                "db_chunk",
                                chunk_pk,
                                overwrite = TRUE)

        #construct a query to replace the processed chunk of table
        query <- paste0('UPDATE ', table_name,
                       ' SET placekey = chunk.placekey',
                       ' FROM (',
                       ' SELECT *',
                       ' FROM db_chunk',
                       ') AS chunk',
                       ' WHERE ', table_name, '.id = chunk.id',
                       ';'
        )

        #execute the query and clear the result
        res <- RPostgres::dbSendQuery(check_connection(dbname, host, port, user),
                                      query,
                                      overwrite = TRUE)
        RPostgres::dbClearResult(res)
        #remove chunk of database
        RPostgres::dbRemoveTable(check_connection(dbname, host, port, user), "db_chunk")

        #calculate lap time
        lap_time <- as.character(lubridate::seconds_to_period(as.numeric(Sys.time()) - as.numeric(lap_start)))

        #log elapsed time for completed query
        logger::log_success('QUERY ', sequence_count, ' COMPLETED AFTER ', as.character(lap_time))

        #get the remaining time
        pk_remainder <- 60 - (as.numeric(Sys.time()) - as.numeric(pk_start))
        #check to see if any of the minute remains if we're not on the last chunk
        if(pk_remainder > 0 & sequence_count != n_chunks){

          #log sleep time
          logger::log_debug('SLEEPING FOR ', pk_remainder, ' SECONDS')

          Sys.sleep(pk_remainder)

          #log restart
          logger::log_debug('RESTARTING...')
        }

        #update the offset
        offset <- limit * sequence_count
        #update the sequence counter
        sequence_count <- sequence_count + 1

      }, silent = TRUE) #end try

    if(class(pk_response) == "try-error") {
      errmsg <- geterrmessage()
      #report error to console
      #cat('>>> ERROR:', errmsg,'\n')
      logger::log_error(errmsg)

      #report error to slack
      slackr::slackr_msg(paste0(':rotating_light:*ERROR ENCOUNTERED ON CHUNK ', sequence_count, ' AT ', as.character(Sys.time()), '*:rotating_light:\n',
                                'somewhere after row ', format(limit * (sequence_count - 1), big.mark = ',', scientific = FALSE, '\n'),
                                '*Message:* ', errmsg, '\n\n',
                                'Please investigate and restart.'))
      break
    }

  } #end while loop

  #report finished time
  total_time <- as.character(lubridate::seconds_to_period(as.numeric(Sys.time()) - as.numeric(master_start)))
  slackr::slackr_msg(paste0('*Query complete:* ', as.character(Sys.time()), '\t*Total time:* ', total_time))
}
