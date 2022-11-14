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
#' @import DBI
#' @import RPostgres
#' @import slackr
#' @importFrom rlang .data
#'
#' @return
#' @export
#'
#' @examples
.pkgglobalenv <- new.env(parent = emptyenv())
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

  #set a primary start time
  master_start <- Sys.time()

  #unused for now
  calculon_quotes <- list(
    initialize = "I'm here to serve. I do owe you for giving me this unholy :man_dancing:`melodramatically`:man_dancing: ACTING talent!\n>AUTHENTICATION SUCCESSFUL",
    pause = 'I better test it with a dramatic... :man_dancing:... pause\n>SLEEPING:',
    fail = "NooOOOoOoOOoooOoo. (It was supposed to be 'yes' but I added my own twist to it.)\n>SOMETHING WENT WRONG:",
    warning = 'NooOOo... Line?\n>PROMPT USER:',
    error = "I'm not familiar with the ... type of _thing_ I'm seeing.\n>SOMETHING WENT WRONG:",
    identify = "I was all of history's great acting Robots: Acting Unit 0.8, Thespo-mat, David Duchovny!\n>STILL RUNNING:",
    success = "Who's that singing at your wedding? It's Calculon, Cal-cu-loooon!.\n>STATUS OK:"
  )

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
      #executes whether verbose or not
      cat('>>> ERROR:', e$message,'\n')
    }

  )

  #assess size database, counting unkeyed addresses
  #convert to integer from integer64 format
  total_rows <- as.integer(RPostgres::dbGetQuery(check_connection(dbname, host, port, user),
                                                 "SELECT count(1) FROM db_test WHERE placekey = '';")$count)

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
  #and again if verbose is true
  if(verbose){
    cat('>>> PROCESSING', format(total_rows, big.mark = ',', scientific = FALSE), 'TOTAL ROWS', 'AT', as.character(Sys.time()),'\n')
  }

  #set up a loop with a 1-indexed counter
  while(sequence_count <= n_chunks){

    #log lap start
    lap_start <- Sys.time()

    #report on query start time
    if(verbose){
      cat('>>> STARTING QUERY', sequence_count, 'AT', as.character(lap_start), '\n')
    }

    #report completed sequence progress in intervals of 10
    if( sequence_count > 1 & (sequence_count %% 10) == 1 ){
      elapsed_time <- as.character(lubridate::seconds_to_period(as.numeric(Sys.time()) - as.numeric(master_start)))
      slackr::slackr_msg(paste0('*Rows processed:* ', format(limit * (sequence_count - 1), big.mark = ',', scientific = FALSE),
                                '\n*Lap start:* ', as.character(Sys.time()), '\t*Elapsed time:* ', elapsed_time
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
            .data$latitude,
            .data$longitude,
            .data$street_address,
            .data$city,
            .data$region,
            .data$postal_code,
            .data$iso_country_code,
            verbose = verbose)
          )

        #write a temporary table to the database
        RPostgres::dbWriteTable(check_connection(dbname, host, port, user),
                                "db_chunk",
                                chunk_pk,
                                overwrite = TRUE)

        #construct a query to replace the processed chunk of table
        query <- paste('UPDATE', table_name,
                       'SET placekey = chunk.placekey',
                       'FROM (',
                       'SELECT *',
                       'FROM db_chunk',
                       ') AS chunk',
                       'WHERE db_test.id = chunk.id',
                       ';',
                       sep = ' '
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
        #report lap time
        if(verbose){
          cat('>>> QUERY', sequence_count, 'COMPLETED AFTER', as.character(lap_time), '\n')
        }

        #get the remaining time
        pk_remainder <- 60 - (as.numeric(Sys.time()) - as.numeric(pk_start))
        #check to see if any of the minute remains if we're not on the last chunk
        if(pk_remainder > 0 & sequence_count != n_chunks){
          if(verbose){
            cat('>>> SLEEPING FOR', pk_remainder, 'SECONDS AT', as.character(Sys.time()),'\n')
          }
          Sys.sleep(pk_remainder)
          if(verbose){
            cat('>>> RESTARTING AT', as.character(Sys.time()), '\n')
          }
        }

        #update the offset
        offset <- limit * sequence_count
        #update the sequence counter
        sequence_count <- sequence_count + 1

      }, silent = TRUE) #end try

    if(class(pk_response) == "try-error") {
      errmsg <- geterrmessage()
      #report error to console
      cat('>>> ERROR:', errmsg,'\n')
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