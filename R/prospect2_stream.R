#' @title p2_stream
#' @description
#'
#' Combined dataset of email sends/clicks/opens/unsubscribes from Prospect2.
#' Features included are:
#'
#' * group_customer_no, customer_no
#' * subscriberid : Prospect2 customer id
#' * timestamp : date of email event
#' * email : email address
#' * event_type : "Email"
#' * event_subtype : "Open", "Click", "Unsubscribe', "Hard Bounce", "Soft Bounce", etc.
#' * campaignid, messageid, listid, linkid : Prospect2 internal ids
#' * ip, ua, uasrc, referer, isread, times : Email open data
#' * status : List subscription status
#'
#' @name p2_stream
NULL

api_url <- "https://brooklynacademyofmusic.api-us1.com"


#' @describeIn p2_query_api get the length of the p2 API table at `url`
p2_query_table_length <- function(url, api_key = keyring::key_get("P2_API")) {

  first <- modify_url(url, query = list("limit" = 1)) %>%
    GET(add_headers("Api-Token" = api_key), httr::timeout(3600)) %>%
    content()
  if (is.null(first$meta)) {
    total <- map_int(first, length) %>% max()
  } else {
    total <- as.integer(first$meta$total)
  }

  total
}

#' p2_query_api
#'
#' Parallel load from P2/Active Campaign API at `url` with key `api_key`. Loads pages of 100 records until it reaches the total.
#'
#' @param url Active Campaign API url to query
#' @param api_key Active Campaign API key, defaults to `keyring::key_get("P2_API")`
#' @param offset integer offset from the start of the query to return; default is 0.
#' @param max_len integer maximum number of rows to load, defaults to
#'    [p2_query_table_length()] - `offset`.
#' @param jobs data.table of jobs to run instead of building a jobs based on `offset` and `max_len`
#'
#' @return JSON object as a list
#' @importFrom httr modify_url GET content add_headers
#' @importFrom checkmate assert check_data_frame check_names
#' @export
p2_query_api <- function(url, api_key = keyring::key_get("P2_API"),
                         offset = NULL, max_len = NULL, jobs = NULL) {
  len <- off <- NULL

  api_headers <- add_headers("Api-Token" = api_key)
  total <- p2_query_table_length(url, api_key)

  if(!is.null(jobs)) {
    assert(check_data_frame(jobs),
           check_names(colnames(jobs), must.include = c("off","len")),
           combine = "and"
    )
    for (var in c("max_len", "offset")) {
      if(!is.null(get(var)))
        warning(paste0("Both `",var,"` and `jobs` are defined, ignoring `",var,"`"))
    }
  } else {

    offset <- offset %||% 0

    if(!is.null(max_len))
      total <- min(max_len + offset, total)

    by <- min(total, 100)

    if (offset >= total)
      return(invisible())

    jobs <- data.table(off = seq(offset, total, by = by))
    jobs <- jobs[, len := c(off[-1], total) - off][len > 0]

  }

  p <- progressor(sum(jobs$len) + 1)
  p(paste("Querying", url))

  mapper <- if(rlang::is_installed("furrr")) {
    furrr::future_map2
  } else {
    purrr::map2
  }

  mapper(jobs$off, jobs$len, ~ {
    res <- make_resilient(GET(modify_url(url, query = list("offset" = .x, "limit" = .y)),
                              api_headers, httr::timeout(3600)) %>%
            content() %>%
            map(p2_json_to_datatable))
    p(amount = .y)
    res
  }) %>% p2_combine_jsons()
}

#' p2_combine_jsons
#'
#' Combine `JSON` objects returned by the Active Campaign API, concatenating each element with the same name.
#'
#' @param jsons list of *named* lists of data.tables, as returned by p2_json_to_datatable
#'
#' @return single `JSON` object as a list, or `NULL` if empty or unnamed
#' @importFrom purrr map_int
#' @importFrom checkmate assert_true
#' @importFrom stats setNames
p2_combine_jsons <- function(jsons) {
  if (!all(map_int(jsons, ~ length(names(.))) > 0))
    return(NULL)

  # combine results
  names <- do.call(c, map(jsons, names)) %>% unique()
  map(setNames(names, names), ~ {
    name <- .
    rbindlist(map(jsons, name), fill = TRUE)
  })
}

#' p2_json_to_datatable
#'
#' Convert P2 JSON objects to a data table
#'
#' @param json list of P2 JSON objects
#' @importFrom purrr map discard keep
#' @importFrom data.table as.data.table
p2_json_to_datatable <- function(json) {
  if (!is.list(json)) {
    return(NULL)
  }

  keep(json, is.list) %>%
    do.call(what = rbind) %>%
    # convert to datatable
    as.data.table()
}

#' p2_db_open
#'
#' Open the local P2 email database
#'
#' @param db_path path of the SQLite database
#'
#' @return invisible
#'
p2_db_open <- function(db_path = tessilake::cache_primary_path("p2.sqlite", "stream")) {

  if (is.null(tessistream$p2_db)) {
    if (!dir.exists(dirname(db_path))) {
      warning(paste("Creating path", dirname(db_path)))
      dir.create(dirname(db_path))
    }

    tessistream$p2_db <- DBI::dbConnect(RSQLite::SQLite(), db_path)
    # Set sqlite timeout to 60 seconds
    RSQLite::sqliteSetBusyHandler(tessistream$p2_db, 60000)
  }

  invisible()
}

#' @describeIn p2_db_open Close the local P2 email database
p2_db_close <- function() {
  if (!is.null(tessistream$p2_db)) {
    DBI::dbDisconnect(tessistream$p2_db)
    tessistream$p2_db <- NULL
  }
  invisible()
}

#' p2_db_update
#'
#' Write to the local P2 email database, either creating a new table or upserting into an existing one
#'
#' @param data data.frame of data to write to the database
#' @param table table name
#' @param overwrite logical whether to delete and overwrite the existing table
#'
#' @return invisible
#' @importFrom checkmate assert_names assert_data_frame
#' @importFrom purrr walk
#' @importFrom dplyr distinct
p2_db_update <- function(data, table, overwrite = FALSE) {
  id <- NULL
  assert_data_table(data)

  if (nrow(data) == 0) {
    return(invisible())
  }

  assert_data_frame(data)
  assert_names(colnames(data), must.include = "id")

  # unnest columns
  for(col in copy(colnames(data)))
      p2_unnest(data,col)

  data <- distinct(data, id, .keep_all = TRUE) %>% 
    filter(!is.na(id))

  if (table %in% DBI::dbListTables(tessistream$p2_db) & !overwrite) {
    sqlite_upsert(tessistream$p2_db, table, data)
  } else {
    dplyr::copy_to(tessistream$p2_db, data, table, unique_indexes = list("id"), temporary = FALSE, overwrite = overwrite)
  }

  invisible()
}

#' p2_unnest
#'
#' Unnest a nested data.table wider. This might be a useful function for
#' other purposes but will need testing. For now it should at least
#' work with the nested structures that come from P2 JSONs
#'
#' @note Assumes for speed that elements of each column are either all named or all unnamed.
#' List columns with variable length are filled with NAs during unnesting.
#'
#' @param data data.table
#' @param colname character, column to unnest
#'
#' @importFrom checkmate assert_data_table assert_choice
#' @importFrom rlang is_atomic list2
#' @importFrom purrr modify modify_if flatten map_lgl
#' @importFrom stats setNames
#' @importFrom data.table rbindlist
#'
#' @return unnested data.table, modified in place (unless the column needs to be unnested longer)
p2_unnest <- function(data, colname) {
  . <- NULL

  assert_data_table(data)
  assert_choice(colname, colnames(data))

  if(data[, is.atomic(get(colname))])
    return(data)

  # Turn length 0 items into NAs
  data[map(get(colname),length) == 0, (colname) := NA]

  if (data[,any(map(get(colname),length)>1)]) {
    new_names <- data[,map(get(colname),names) %>% unlist %>% unique]
    new_width <- max(length(new_names),
      data[,map(get(colname),length) %>% unlist %>% max])
    if (is.null(new_names)) {
      data[ map(get(colname),length) < new_width,
            (colname) := map(get(colname), ~c(as.list(.),
                                              rep(NA,new_width-length(.))))]
    } else {
      data[ map(get(colname),~ is.null(names(.))) == TRUE,
            (colname) := map(get(colname),
                             ~modifyList(setNames(rep(list(NA),new_width),
                                                  new_names),as.list(.)))]

    }
    data[, paste(colname, new_names %||% seq(new_width), sep = ".") :=
           rbindlist(get(colname), fill = !is.null(new_names))]
    data[,(colname) := NULL]
  } else {
    data[,(colname) := unlist(get(colname))]
  }

}

#' p2_update
#'
#' Incrementally update all of the p2 data in the local sqlite database
#'
#' @importFrom dplyr tbl summarize collect filter full_join select group_by transmute tally
#' @importFrom lubridate today dmonths
#' @export
p2_update <- function() {
  updated_timestamp <- id <- linkclicks <- sdate <- campaignid <- NULL

  withr::defer(p2_db_close())
  p2_db_open()

  # not immutable or filterable, just reload the whole thing
  p2_load("campaigns")
  p2_load("messages")
  p2_load("links")
  p2_load("lists")
  p2_load("bounceLogs")
  p2_load("contactLists", overwrite = TRUE)
  p2_load("fieldValues", path = "api/3/fieldValues", query = list("filters[fieldid]" = 1))

  # has a date filter
  contacts_max_date <- if (DBI::dbExistsTable(tessistream$p2_db, "contacts")) {
    tbl(tessistream$p2_db, "contacts") %>%
      summarize(max(updated_timestamp, na.rm = TRUE)) %>%
      collect()
  } else {
    "1900-01-01"
  }
  p2_load("contacts", query = list("filters[updated_after]" = as.character(contacts_max_date)))

  # then load new log entries greater than the current max id
  for (table in c("logs", "linkData", "mppLinkData")) {
    len <- if (DBI::dbExistsTable(tessistream$p2_db, table)) {
      tbl(tessistream$p2_db, table) %>%
        tally %>% collect %>%
        as.integer
    } else {
      0
    }
    p2_load(table, offset = len)
  }
}

#' p2_load
#'
#' Load p2 data from `api/3/{table}`, modified by arguments in `...` to the matching `table` in the local database
#'
#' @param table character, table to update
#' @param overwrite logical, whether to overwrite the table in the database
#' @inheritParams p2_query_api
#' @param ... additional parameters to pass on to modify_url
#'
#' @importFrom rlang list2 `%||%` call2
p2_load <- function(table, offset = NULL, max_len = NULL,
                    jobs = NULL, overwrite = FALSE, ...) {
  . <- NULL

  # fresh load of everything
  args <- list2(...)
  args$path <- args$path %||% paste0("api/3/", table)
  args$url <- args$url %||% api_url

  data <- p2_query_api(eval(call2("modify_url", !!!args)), offset = offset, max_len = max_len)

  if(!is.null(data[[table]])) {
    p2_db_update(data[[table]], table, overwrite = overwrite)
  }
}

#' p2_email_map
#'
#' Constructs a many-to-many mapping between email addresses / P2 subscriber ids and Tessi customer numbers
#'
#' @return data.table of a mapping between email addresses and customer numbers
#' @export
#' @importFrom dplyr distinct select transmute
#' @importFrom data.table setDT first
p2_email_map <- function() {

  primary_ind <- address <- customer_no <- . <- email <- id <- value <- 
    contact <- i.customer_no <- group_customer_no <- i.group_customer_no <- NULL

  # load data
  emails <- tessilake::read_tessi("emails",freshness = 0) %>%
    filter(primary_ind == "Y") %>%
    select(email = address, customer_no) %>%
    collect() %>%
    setDT() %>% .[,email:=tolower(trimws(email))]

  contacts <- tbl(tessistream$p2_db, "contacts") %>%
    transmute(
      id = as.integer(id),
      email = tolower(trimws(email))
    ) %>%
    collect() %>%
    setDT()

  customer_nos <- tbl(tessistream$p2_db, "fieldValues") %>%
    transmute(
      customer_no = as.integer(value),
      id = as.integer(contact)
    ) %>%
    collect() %>%
    setDT()

  # data between Tessi and p2 is *eventually* consistent but can get out of sync.
  # - if someone changes their email address in Tessi it might not get changed in P2
  # in that case, we want to rely on the customer number saved in P2 because that still
  # points to the correct customer.
  # - if someone changes their email address in P2 it might not get changed in Tessi
  # in that case we want to rely on the customer number saved in P2, because that still
  # points to the correct customer.
  # - email addresses are unique in P2, customer numbers are unique in Tessi. In the
  # case where multiple customers point to one email address, we want to expand to
  # all of the customer numbers so that we are tracking behavior across accounts...

  # link P2 customer_nos and contacts
  contacts[customer_nos, `:=`(
    customer_no = i.customer_no
  ), on = "id"]

  # all P2 contacts with either email or customer #
  contacts <- contacts[!is.na(customer_no) | !is.na(email)]

  # all P2 contacts + all tessi contacts with emails in P2
  email_map <- rbind(contacts, 
                     emails[contacts$email,,on="email"][!is.na(customer_no)], 
                     fill = T)

  # fill in subscriber id for newly added tessi emails
  email_map <- email_map[,id:=first(id),by="email"]

  # map customer_no -> group_customer_no
  email_map[tessilake::tessi_customer_no_map() %>%
              collect() %>%
              setDT(), group_customer_no := i.group_customer_no, on = "customer_no"]


  distinct(email_map)
}

#' p2_stream_build
#'
#' @return p2_stream as data.table
#'
#' @importFrom data.table setDT
#'
p2_stream_build <- function() {

  unixepoch <- tstamp <- subscriberid <- campaignid <- messageid <- link <- isread <- times <- ip <- ua <-
    uasrc <- referer <- email <- status <- updated_timestamp <- contact <- campaign <- event_subtype <- timestamp <- NULL

  # group_customer_no
  # timestamp : date of email event
  # event_type : Email
  # event_subtype : Open|Click|Unsubscribe|Hard Bounce|Forward
  # event_subtype2 : Onsale|Newsletter|Film|Fundraising
  # campaign_desc
  # url
  # domain

  withr::defer(p2_db_close())
  p2_db_open()

  sends <- tbl(tessistream$p2_db, "logs") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid),
      campaignid = as.integer(campaignid),
      messageid = as.integer(messageid)
    ) %>%
    collect() %>%
    setDT()

  events <- tbl(tessistream$p2_db, "linkData") %>%
    filter(messageid != "0") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid),
      campaignid = as.integer(campaignid),
      messageid = as.integer(messageid),
      linkid = as.integer(link),
      isread = as.logical(isread),
      times = as.integer(times),
      ip, ua, uasrc, referer
    ) %>%
    collect() %>%
    setDT()

  events2 <- tbl(tessistream$p2_db, "mppLinkData") %>%
    filter(messageid != "0") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid),
      campaignid = as.integer(campaignid),
      messageid = as.integer(messageid),
      linkid = as.integer(link),
      isread = as.logical(isread),
      times = as.integer(times),
      ip, ua, uasrc, referer
    ) %>%
    collect() %>%
    setDT()

  events <- rbind(events, events2, fill = T)

  bounces <- tbl(tessistream$p2_db, "bounceLogs") %>%
    filter(email!="") %>%
    transmute(
      timestamp = unixepoch(tstamp),
      subscriberid = as.integer(subscriberid)
    ) %>%
    collect() %>%
    setDT()

  unsubs <- tbl(tessistream$p2_db, "contactLists") %>%
    filter(status != "1") %>%
    transmute(
      timestamp = unixepoch(updated_timestamp),
      status = as.integer(status),
      subscriberid = as.integer(contact),
      campaignid = as.integer(campaign),
      messageid = as.integer(message),
      listid = as.integer(list)
    ) %>%
    collect() %>%
    setDT()

  p2_stream <- rbind(
    bounces[, event_subtype := "Soft Bounce"],
    unsubs[, event_subtype := ifelse(status == 2, "Unsubscribe", "Hard Bounce")],
    events[, event_subtype := ifelse(isread, "Open", "Click")],
    sends[, event_subtype := "Send"],
    fill = T)

  setkey(p2_stream, subscriberid, timestamp)
  p2_stream[, `:=`(timestamp = lubridate::as_datetime(timestamp),
                   event_type = "Email")]

  p2_stream <- merge(p2_stream, p2_email_map(), by.x="subscriberid", by.y="id", all.x=T, allow.cartesian = TRUE)

  p2_stream
}

#' p2_stream_enrich
#'
#' @param p2_stream stream data.table from p2_stream_build
#' @importFrom tessilake setleftjoin
#' @importFrom data.table setDT
#' @return stream data.table with added descriptive columns
p2_stream_enrich <- function(p2_stream) {
  withr::defer(p2_db_close())
  p2_db_open()

  name <- screenshot <- id <- send_amt <- total_amt <- opens <- uniqueopens <- linkclicks <- uniquelinkclicks <-
    subscriberclicks <- forwards <- uniqueforwards <- hardbounces <- softbounces <- unsubscribes <- . <- subject <-
    preheader_text <- link <- NULL

  campaigns <- tbl(tessistream$p2_db, "campaigns") %>% select(campaign_name=name,screenshot,
                                                              id,send_amt,total_amt,opens,uniqueopens,linkclicks,uniquelinkclicks,
                                                              subscriberclicks,forwards,uniqueforwards,hardbounces,softbounces,unsubscribes) %>%
    collect %>%
    setDT %>%
    .[,(3:ncol(.)):=lapply(.SD,as.integer),.SDcols=3:ncol(.)]
  messages <- tbl(tessistream$p2_db, "messages") %>% transmute(id = as.integer(id),message_name=name,subject,preheader_text) %>% collect %>% setDT
  links <- tbl(tessistream$p2_db, "links") %>% transmute(id = as.integer(id),link_name=name,link) %>% collect %>% setDT
  lists <- tbl(tessistream$p2_db, "lists") %>% transmute(id = as.integer(id),list_name=name) %>% collect %>% setDT

  p2_stream <- setleftjoin(p2_stream,campaigns,by=c("campaignid"="id"))
  p2_stream <- setleftjoin(p2_stream,messages,by=c("messageid"="id"))
  p2_stream <- setleftjoin(p2_stream,links,by=c("linkid"="id"))
  p2_stream <- setleftjoin(p2_stream,lists,by=c("listid"="id"))

  p2_stream
}


#' @describeIn p2_stream Update and build the p2 parquet files
#'
#' @return stream as a data.table
#' @importFrom tessilake write_cache sync_cache
#' @export
p2_stream <- function() {

  p2_stream <- p2_stream_build()
  write_cache(p2_stream,"p2_stream","stream",overwrite = T)
  p2_stream_enriched <- p2_stream_enrich(p2_stream)
  write_cache(p2_stream,"p2_stream_enriched","stream",overwrite = T)

  sync_cache("p2.sqlite", "stream")

  p2_stream

}
