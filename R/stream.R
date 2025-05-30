

#' stream
#' 
#' Combine all streams named in `streams` into a single dataset, filling down columns matched by `fill_match` and 
#' creating windowed features using `windows` as offsets for columns matched by `window_match`. 
#' The full dataset is rebuilt if `rebuild` is `TRUE` (default: `FALSE`), and data is appended to the
#' existing cache if `incremental` is `TRUE` (default: `TRUE`)
#'
#' @param streams [character] vector of streams to combine
#' @param fill_match [character](1) regular expression to use when matching columns to fill down 
#' @param window_match [character](1) regular expression to use when matching columns to window
#' @param since [POSIXct](1) date after which to build the stream
#' @param until [POSIXct](1) date until which to build the stream
#' @param rebuild [logical](1) whether or not to rebuild the whole dataset (`TRUE`) or just append to the end of it (`FALSE`)
#' @param incremental [logical](1) whether or not to update the cache incrementally. Can require huge amounts of memory (approximately double the total dataset size to be appended).
#' @importFrom data.table setDT
#' @importFrom dplyr collect filter transmute
#' @importFrom tessilake read_cache cache_exists_any write_cache sync_cache
#' @importFrom checkmate assert_character assert_logical assert_list assert_posixct
#' @importFrom lubridate as_datetime now years
#' @param ... not used
#' @param windows 
#'
#' @return stream dataset as an [arrow::Table]
#' @export
stream <- function(streams = c("email_stream","ticket_stream","contribution_stream",
                                "membership_stream","ticket_future_stream","address_stream"),
                   fill_match = "^(email|ticket|contribution|membership|ticket|address).+(amt|level|count|max|min|last)",
                   window_match = "^(email|ticket|contribution|membership|ticket|address).+(count|amt)$",
                   rebuild = FALSE, 
                   since = now() - dyears(),
                   until = now() + dyears(10),
                   incremental = !rebuild, 
                   windows = lapply(c(1,7,30,90,365),
                                    lubridate::period,
                                    units = "day"), ...) {
  
  . <- timestamp <- NULL
  
  assert_character(streams,min.len = 1)
  assert_character(fill_match,len=1)
  assert_character(window_match,len=1)
  assert_logical(rebuild)
  assert_posixct(until,len=1)
  assert_posixct(since,len=1)
  
  # load stream headers
  streams <- lapply(setNames(nm = streams), \(stream) read_cache(stream, "stream", include_partition = T))
  
  # match columns by name
  fill_cols <- lapply(streams, colnames) %>% unlist %>% unique %>% 
    grep(pattern = fill_match, ignore.case = T, perl = T, value = T)
  window_cols <- fill_cols %>% grep(pattern = window_match, ignore.case = T, perl = T, value = T)
  
  rlang::inform(c("i" = "planning partitions"))
  stream_max_date <- as_datetime("1900-01-01")
  if(cache_exists_any("stream","stream") & !rebuild) {
    stream <- read_cache("stream","stream")
    stream_max_date <- since
  }
    
  partitions <- lapply(streams, \(stream) 
                       filter(stream, timestamp > stream_max_date &
                                      timestamp < until) %>% 
                         group_by(partition = as_datetime(floor_date(timestamp,"year"))) %>% 
                         summarize %>% 
                         collect) %>% 
    rbindlist %>% distinct %>% .[!is.na(partition)]

  setkey(partitions,partition)
  partitions[,timestamp := partition+years()]
  
  for(partition in split(partitions, partitions$partition)) {
    
    rlang::inform(c(paste("Building stream to date",partition$timestamp),
                    "i" = "loading data"))
    
    # load data from streams
    stream <- lapply(streams, \(stream) filter(stream, timestamp > stream_max_date & 
                                                       timestamp < partition$timestamp) %>% 
                       mutate(timestamp = as_datetime(timestamp),
                              timestamp_id = arrow:::cast(timestamp,arrow::int64())) %>%
                       collect %>% setDT) %>% 
      rbindlist(idcol = "stream", fill = T) %>% 
      .[,`:=` (partition = partition$partition)]
    
    stream_max_date = as_datetime(max(stream$timestamp))

    if(nrow(stream) == 0) 
      next
    
    # do the filling and windowing
    stream_chunk_write(stream, fill_cols = fill_cols, window_cols = window_cols,
                       since = min(stream$timestamp),
                       incremental = incremental, windows = windows, ...)
    
    gc()
  }
  
  sync_cache("stream", "stream", overwrite = TRUE, partition = "partition")
  
}


#' @describeIn stream Fill down cols in `stream_cols` and add windowed features to `stream` for timestamps after `since`
#' @importFrom checkmate assert_data_table assert_names assert_posixct assert_character assert_list
#' @importFrom lubridate as_datetime
#' @importFrom dplyr coalesce
#' @param stream [data.table] data to process and write 
#' @param fill_cols [character] columns to fill down 
#' @param window_cols [character] columns to window
#' @param since [POSIXct] only the data with timestamps greater than or equal to `since` will be written
#' @param by [character](1) column name to group by for filling down and windowing
stream_chunk_write <- function(stream, fill_cols = setdiff(colnames(stream),
                                                             c(by, "timestamp")),
                               window_cols = fill_cols,
                               since = as_datetime(min(stream$timestamp)), 
                               by = "group_customer_no",
                               incremental = TRUE,
                               ...) {
  
  timestamp <- group_customer_no <- rowid <- NULL
  
  assert_data_table(stream)
  assert_names(colnames(stream), must.include = c(by,"timestamp", fill_cols))
  assert_posixct(since,len=1)
  assert_character(by,len=1)
  
  # load the last row per customer for filling down
  stream_prev <- stream_customer_history <- data.table()
  if(cache_exists_any("stream","stream")) {
    
    rlang::inform(c(i = "loading customer history"))
    stream_customer_history <- stream_customer_history(read_cache("stream","stream"), 
                                                       by = by, 
                                                       before = since,
                                                       pattern = paste0("^",fill_cols,"$"))
  }
  
  stream <- rbind(stream_customer_history,
                  stream, fill = T) 
  rm(stream_customer_history)
  setkey(stream, group_customer_no, timestamp)
  
  rlang::inform(c(i = "filling down"))
  # fill down
  setnafill(stream, type = "locf", cols = fill_cols, by = by)
  
  # load the last year for windowing
  max_rowid <- 0
  if(cache_exists_any("stream","stream")) {
    
    rlang::inform(c(i = "loading previous year"))
    stream_prev <- read_cache("stream", "stream") %>% 
      filter(as_datetime(timestamp) >= as_datetime(since - dyears()) & 
               as_datetime(timestamp) < as_datetime(since)) %>% 
      select(all_of(c(by,"timestamp","rowid")),matches(paste0("^",fill_cols,"$"))) %>% 
      collect %>% setDT
    
    max_rowid <- read_cache("stream","stream") %>% 
      filter(timestamp < as_datetime(since)) %>%
      summarize(max(rowid)) %>% collect %>% as.numeric() %>% coalesce(0)
  }
  
  stream <- rbind(stream_prev,
                  stream[timestamp >= since], fill=T)
  rm(stream_prev)
  setkey(stream, group_customer_no, timestamp)

  rlang::inform(c(i = "windowing"))
  # window
  stream <- stream_window_features(stream, window_cols = window_cols, by = by, since = since, ...)
  
  rlang::inform(c(v = "writing cache"))
  setkey(stream,timestamp)
  stream[timestamp >= since,rowid:=max_rowid+seq(.N)]
  # save
  setkey(stream, rowid)
  args <- list(x = stream[timestamp >= since],
               table_name = "stream",
               type = "stream",
               partition = "partition",
               sync = FALSE,
               incremental = incremental)
  
  if(incremental) {
    args$date_column = "timestamp"
  } else {
    args$overwrite = TRUE
  }
  
  do.call(write_cache, args)
}

#' @describeIn stream construct windowed features for columns in `stream_cols`, using `windows`, 
#' a list of [lubridate::period]s as offsets, and grouped by `by`
#' @param windows [lubridate::period] vector that determines the offsets used when constructing the windowed features.
stream_window_features <- function(stream, window_cols = setdiff(colnames(stream), 
                                                                 c(by,"timestamp")),
                                   since = min(stream$timestamp),
                                   windows = NULL, by = "group_customer_no", ...) {
  
  . <- timestamp <- group_customer_no <- NULL
  
  assert_data_table(stream)
  assert_names(colnames(stream), must.include = c(by, "timestamp",window_cols))
  assert_names(window_cols, disjunct.from = c(by, "timestamp"))
  assert_list(windows, types = "Period", null.ok = TRUE)
  
  if (length(windows) > 0) {
    # sort windows
    windows <- windows[order(purrr::map_dbl(windows, as.numeric))]
  }
  
  setkey(stream, group_customer_no, timestamp)
  stream_window <- stream[,c(by,"timestamp",window_cols),with=F] %>% 
    stream_debounce(by,"timestamp")
  stream <- stream[timestamp >= since]
  stream_key <- stream[,c(by,"timestamp"),with=F]
  
  for (window in windows) {
      # rolling join with adjusted stream
      stream_rolled = copy(stream_window) %>% .[,timestamp := timestamp + window]
      stream_rolled <- stream_rolled[stream_key, on = c(by, "timestamp"), roll = Inf]
      
      # subtract columns and add to stream
      new_cols <- paste0(window_cols, ".-", as.numeric(window)/86400)
      stream[,(new_cols) := purrr::map(window_cols, \(col) get(col)-stream_rolled[,get(col)])]
  }
  
  # work backwards through windowed features and subtract each from the next (lower offset) to 
  # get properly decoupled features
  for (i in rev(seq_along(windows)[-1])) {
    window <- windows[[i]]
    prev_window <- windows[[i-1]]
    
    new_cols <- paste0(window_cols, ".-", as.numeric(window)/86400)
    prev_cols <- paste0(window_cols, ".-", as.numeric(prev_window)/86400)
    
    stream[,(new_cols) := purrr::map2(new_cols, prev_cols, \(.x,.y) get(.x)-get(.y))]
  }
  
  stream
}
