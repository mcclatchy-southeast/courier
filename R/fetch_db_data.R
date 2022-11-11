#' Retrieve a chunk of data
#'
#' Use Postgres to access a specified database and retrieve a chunk of data
#' in a specified size as a `dplyr` tibble.
#'
#' @param con Pass in a connection without reestablishing one. Default is `NULL`.
#' @param dbname Name of the database. Default is `NULL`.
#' @param host Name of the host IP address. Default is `NULL`.
#' @param user Username. Default is `NULL`.
#' @param port Port number, typically 5432. Default is `NULL`.
#' @param table_name Name of the table you want to query data
#' @param order Default is `1`, or the data's first column.
#' @param limit Default is `10000`, consistent with Placekey bulk API rate limiting.
#' @param offset Number of rows to skip in the data. Default is `0`.
#'
#' @import DBI
#' @import RPostgres
#' @import dplyr
#' @importFrom dplyr %>%
#' @importFrom rlang .data
#'
#' @return A tibble chunked from the specified database table.
#' @export
#'
#' @examples
#' \dontrun{
#' fetch_db_data(table_name = 'mtcars', offset = 5)
#' fetch_db_data('exampledb', '123.456.7.89', 'user', table_name = 'mtcars')
#' }
fetch_db_data <- function(con = NULL,
                          dbname = NULL,
                          host = NULL,
                          user = NULL,
                          port = NULL,
                          table_name,
                          order = 1,
                          limit = 10000,
                          offset = 0){

  #establish the connection if it doesn't exist already
  if(is.null(con)){
    con <- DBI::dbConnect(
      drv = RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      port = port,
      user = user)
  }

  #construct the query, only fetching empty placekeys
  query = paste('SELECT *',
                'FROM', table_name,
                'ORDER BY', order,
                'LIMIT', limit, 'OFFSET', offset,
                ';',
                sep = ' ')

  #get the data from the specified table
  #limit to only rows without placekeys
  df_chunk <- dplyr::tibble(dbGetQuery(con, query)) %>%
    dplyr::filter(.data$placekey == '')

  #disconnect from the database
  dbDisconnect(con)

  return(df_chunk)

}
