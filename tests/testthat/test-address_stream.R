withr::local_package("mockery")
withr::local_package("dplyr")

audit <- readRDS(test_path("address_audit.Rds"))
addresses <- readRDS(test_path("addresses.Rds"))
# Use this to turn off libpostal execution
# stub(address_exec_libpostal,"Sys.which","")


# address_stream ----------------------------------------------------------

test_that("address_stream_build combines data from address_parse, address_census, address_geocode, and read_tessi and returns all rows and a subset of columns", {
  tessilake::local_cache_dirs()

  address_stream_original <- readRDS(rprojroot::find_testthat_root_file("address_stream.Rds"))
  stub(address_stream_build, "address_create_stream", address_stream_original)
  stub(address_stream_build, "address_parse", cbind(distinct(address_stream_original[,..address_cols]),libpostal =
        data.table()[,c("house_number", "road", "unit", "house", "po_box", "city", "state", "country", "postcode") := NA_character_]))

  address_geocode <- readRDS(rprojroot::find_testthat_root_file("address_geocode.Rds"))
  stub(address_stream_build, "address_geocode", address_geocode[address_stream_original[,..address_cols],on=address_cols])

  address_stream_normalized <- cbind(address_stream_original[,..address_cols],
                                     setnames(address_stream_original[,..address_cols],
                                              address_cols, paste0(address_cols,"_cleaned")))
  stub(address_stream_build, "address_normalize", address_stream_normalized)
  stub(address_reverse_census,"address_geocode",address_geocode)
  stub(address_census,"address_reverse_census",address_reverse_census)
  stub(address_census, "census_features", readRDS(rprojroot::find_testthat_root_file("census_features.Rds")))
  stub(address_stream_build, "address_census", address_census)

  iwave <- readRDS(rprojroot::find_testthat_root_file("address_iwave.Rds"))
  stub(address_stream_build, "read_tessi", iwave)

  suppressMessages(address_stream <- address_stream_build() %>% collect)

  # output is the same length as input
  expect_equal(nrow(address_stream),
               nrow(address_stream_original))

  # the correct iwave data was appended
  expect_equal(address_stream[!is.na(address_pro_score_level),.N],
               address_stream[iwave, on = "group_customer_no"][timestamp > score_dt,.N])

  # has all the stream column names
  checkmate::expect_names(colnames(address_stream), must.include = c(address_cols, paste0(address_cols,"_cleaned"),
                                                                     "lat", "lon", "address_no",
                                                  "event_type", "event_subtype", "group_customer_no", "timestamp"))


  # has all the census data appended
  expect_equal(sum(grepl("median_income|over_65|male|african_american",colnames(address_stream))),4)

  .address_stream <<- address_stream
})

test_that("address_stream writes a full file containing additional data", {
  tessilake::local_cache_dirs()
  stub(address_stream, "address_stream_build", .address_stream)
  stub(address_stream, "sync_cache", TRUE)

  address_stream()

  address_stream <- read_cache("address_stream", "stream", num_tries = 1)
  address_stream_full <- read_cache("address_stream_full", "stream", num_tries = 1)

  expect_gt(nrow(address_stream_full), nrow(address_stream))
  expect_gt(ncol(address_stream_full), ncol(address_stream))
  expect_true(all(colnames(address_stream) %in% c(address_cols, "lat", "lon",
                                                  "event_type", "event_subtype", "group_customer_no", "address_no",
                                                  "timestamp", "address_no") |
                  grepl("^address.+level$",colnames(address_stream))))

})

test_that("address_stream copies the address database", {
  tessilake::local_cache_dirs()

  withr::local_package("checkmate")
  stub(address_stream, "address_stream_build", .address_stream)

  # create an empty database
  dir.create(tessilake::cache_primary_path("","stream"))
  file.create(tessilake::cache_primary_path("address_stream.sqlite","stream"))

  address_stream()

  expect_file_exists(tessilake::cache_path("address_stream.sqlite","deep","stream"))
  expect_file_exists(tessilake::cache_path("address_stream.sqlite","shallow","stream"))
})
.address_stream <<- NULL

# address_create_stream ---------------------------------------------------

test_that("address_create_stream builds a stream using all data from audit table", {
  read_tessi <- mock(audit,addresses[1])
  stub(stream_from_audit, "read_tessi", read_tessi)
  stub(address_create_stream, "stream_from_audit", stream_from_audit)
  expect_equal(
    nrow(address_create_stream()),
    nrow(distinct(audit[, .(alternate_key, lubridate::as_date(date))])) + 1
  )
})

test_that("address_create_stream builds a stream using all data from address table", {
  read_tessi <- mock(audit[1],addresses)
  stub(stream_from_audit, "read_tessi", read_tessi)
  stub(address_create_stream, "stream_from_audit", stream_from_audit)
  expect_equal(nrow(address_create_stream()),
               addresses[,.(address_no,create_dt,last_update_dt)] %>%
                 data.table::melt(id.vars="address_no") %>%
                 distinct(address_no,as_date(value)) %>% nrow + 1)
})

test_that("address_create_stream has some data in each row, including creations", {
  read_tessi <- mock(audit,addresses)
  stub(stream_from_audit, "read_tessi", read_tessi)
  stub(address_create_stream, "stream_from_audit", stream_from_audit)

  stream <- address_create_stream()
  missing_data <- stream[purrr::reduce(lapply(mget(address_cols), is.na), `&`)]

  expect_equal(nrow(missing_data[action!="DELETED"]), 0)
})

test_that("address_create_stream fills in all data", {
  read_tessi <- mock(audit,addresses)
  stub(stream_from_audit, "read_tessi", read_tessi)
  stub(address_create_stream, "stream_from_audit", stream_from_audit)

  stream <- address_create_stream()
  audit[, address_no := as.integer(alternate_key)]

  missing_data <- data.table::melt(stream, measure.vars = c(address_cols,"primary_ind","inactive"))[is.na(value)]
  # none of these data exist in the audit table
  expect_equal(audit[missing_data, , on = c("address_no", "column_updated" = "variable")] %>%
                 .[!is.na(date), .N], 0)

  # none of these data exist as the latest address data
  latest_data <- data.table::melt(addresses, measure.vars = c("street1","street2","city",state = "state_short_desc",
                                                              "postal_code",country = "country_short_desc"))[!is.na(value)]
  expect_equal(latest_data[missing_data, , on = c("address_no", "variable")] %>%
                 .[!is.na(create_dt), .N], 0)

})

test_that("address_create_stream returns only one address change per day", {
  read_tessi <- mock(audit,addresses)
  stub(stream_from_audit, "read_tessi", read_tessi)
  stub(address_create_stream, "stream_from_audit", stream_from_audit)

  stream <- address_create_stream()

  expect_equal(stream[, .N, by = c("timestamp", "address_no")][N > 1, .N], 0)
})

# address_exec_libpostal --------------------------------------------------

test_that("address_exec_libpostal complains if addresses are not a character vector", {
  expect_error(address_exec_libpostal(c(1, 2, 3, 4)), "addresses.+character")
  expect_error(address_exec_libpostal(NA), "addresses.+missing")
})
test_that("address_exec_libpostal complains if libpostal can't be found", {
  stub(address_exec_libpostal, "Sys.which", "")
  expect_error(address_exec_libpostal("test"), "address_parser.+not found")
})
test_that("address_exec_libpostal gets back data from libpostal", {
  expect_mapequal(
    address_exec_libpostal("Brooklyn Academy of Music, 30 lafayette ave, brooklyn, ny 11217"),
    data.frame(
      house_number = "30",
      road = "lafayette ave",
      house = "brooklyn academy of music",
      city_district = "brooklyn",
      state = "ny",
      postcode = "11217"
    )
  )
})
test_that("address_exec_libpostal handles UTF-8", {
  expect_mapequal(
    address_exec_libpostal("92 Ave des Champs-Élysées"),
    data.frame(
      house_number = "92",
      road = "ave des champs-élysées"
    )
  )
})

# address_parse_libpostal ------------------------------------------------

test_that("address_parse_libpostal sends a lowercase character vector to libpostal", {
  libpostal <- mock(rlang::abort(class = "libpostal"))
  stub(address_parse_libpostal, "address_exec_libpostal", libpostal)

  address_stream <- expand.grid(
    street1 = c("Brooklyn Academy of Music", "30 Lafayette Ave"),
    street2 = c("30 Lafeyette Ave", "4th Floor"),
    city = c("Brooklyn", "NY"),
    state = "NY",
    postal_code = "11217",
    country = "",
    primary_ind = "Y"
  ) %>% setDT

  expect_error(address_parse_libpostal(address_stream), class = "libpostal")

  expect_equal(length(mock_args(libpostal)[[1]][[1]]), 8)
  expect_equal(mock_args(libpostal)[[1]][[1]], tolower(mock_args(libpostal)[[1]][[1]]))
  expect_equal(mock_args(libpostal)[[1]][[1]], unique(mock_args(libpostal)[[1]][[1]]))
})

test_that("address_parse_libpostal handles unit #s hidden in postalcode, street2, and road", {
  address_stream <- data.table()[, (address_cols) := rep(NA, 8)][, `:=`(
    street1 = paste(1:8,"Main Street"),
    street2 = c("4k", rep(NA, 7)),
    postal_code = rep("11217", 8)
  )]

  libpostal_return <- data.table(
    "road" = c(rep("lafayette ave", 7), "lafayette ave w apt 4g"),
    "unit" = rep(NA_character_, 8),
    "postcode" = c(NA, "11217", "4g", "33", "a4", "ph33", "fl. 4", NA)
  )

  stub(address_parse_libpostal, "address_exec_libpostal", libpostal_return)

  parsed <- address_parse_libpostal(address_stream)

  expect_equal(parsed$libpostal.postcode, c(NA, "11217", rep(NA, 6)))
  expect_equal(parsed$libpostal.unit, c("4k", NA, "4g", "33", "a4", "ph33", "fl. 4", "apt 4g"))
  expect_equal(parsed$libpostal.road, c(rep("lafayette ave", 7), "lafayette ave w"))
})
test_that("address_parse_libpostal cleans up duplicated unit #s", {
  address_stream <- data.table()[, (address_cols) := rep(NA, 8)][, `:=`(street1 = paste(1:8,"Main Street"))]

  libpostal_return <- data.table(
    "road" = c("lafayette ave", "lafayette st e"),
    "unit" = c("4k", "", "4g", "33", "a4", "ph33", "fl. 4", "apt 4g"),
    "postcode" = c(NA, "11217", "4g", "11233", "a4", "ph33", "fl. 4", NA)
  ) %>%
    .[, `:=`(
      road = trimws(paste(road, unit)),
      house = unit
    )]

  stub(address_parse_libpostal, "address_exec_libpostal", libpostal_return)

  parsed <- address_parse_libpostal(address_stream)

  expect_equal(parsed$libpostal.postcode, c(NA, "11217", NA, "11233", rep(NA, 4)))
  expect_equal(parsed$libpostal.unit, c("4k", NA, "4g", "33", "a4", "ph33", "fl. 4", "apt 4g"))
  expect_equal(parsed$libpostal.house, rep(NA_character_, 8))
  expect_equal(parsed$libpostal.road, rep(c("lafayette ave", "lafayette st e"), 4))
})
test_that("address_parse_libpostal returns only house_number,road,unit,house,po_box,city,state,country,postcode but handles all columns", {
  libpostal_cols <- c(
    "house", "category", "near", "house_number", "road", "unit", "level", "staircase",
    "entrance", "po_box", "postcode", "suburb", "city_district", "city", "island",
    "state_district", "state", "country_region", "country", "world_region"
  )

  libpostal_return <- structure(libpostal_cols, names = libpostal_cols) %>%
    as.list() %>%
    setDT() %>%
    .[rep(1, 8)]
  address_stream <- data.table()[, (address_cols) := rep(NA, 8)][, `:=`(street1 = paste(1:8,"Main Street"))]
  stub(address_parse_libpostal, "address_exec_libpostal", libpostal_return)

  parsed <- address_parse_libpostal(address_stream)

  expect_named(parsed, c(
    address_cols, paste0(
      "libpostal.",
      c("house_number", "road", "unit", "house", "po_box", "city", "state", "country", "postcode")
    )
  ), ignore.order = TRUE)

  expect_equal(parsed$libpostal.house_number, rep("house_number", 8))
  expect_equal(parsed$libpostal.unit, rep(paste("unit", "level", "entrance", "staircase"), 8))
  expect_equal(parsed$libpostal.house, rep(paste("house", "category", "near"), 8))
  expect_equal(parsed$libpostal.city, rep(paste("suburb", "city_district", "city", "island", sep = ", "), 8))
  expect_equal(parsed$libpostal.state, rep(paste("state_district", "state", sep = ", "), 8))
  expect_equal(parsed$libpostal.country, rep(paste("country_region", "country", "world_region", sep = ", "), 8))
})


# address_cache -----------------------------------------------------------
tessilake::local_cache_dirs()

address_stream <- data.table(street1 = paste(1:100, "example lane"), something = "extra") %>% rbind(setNames(rep(list(character(0)),6),address_cols),
                                                                                                    fill = TRUE)
address_result <- data.table::copy(address_stream)[, processed := strsplit(street1, " ") %>% purrr::map_chr(1)]

sqlite_file <- tessilake::cache_primary_path("address_stream.sqlite", "stream")

local_db <- function(sqlite_file) {
  db <- DBI::dbConnect(RSQLite::SQLite(), sqlite_file)
  withr::defer(DBI::dbDisconnect(db),parent.frame())
  db
}

test_that("address_cache handles cache non-existence and writes a cache file containing only address_processed cols", {
  expect_false(file.exists(sqlite_file))
  address_processor <- mock(address_result[1:25])
  address_cache(address_stream[1:25], "address_cache", address_processor)
  expect_true(file.exists(sqlite_file))
  db <- local_db(sqlite_file)
  expect_named(DBI::dbReadTable(db, "address_cache"), c(address_cols, "something", "processed"), ignore.order = TRUE)
})

test_that("address_cache adds an index to the table", {
  db <- local_db(sqlite_file)
  expect_equal(nrow(DBI::dbGetQuery(db, "select * from sqlite_master where type = 'index'")),1)
})

test_that("address_cache handles zero-row input gracefully", {
  db <- local_db(sqlite_file)
  address_processor <- mock(address_result[0])
  address_cache(address_stream[0], "address_cache", address_processor)
  expect_equal(nrow(DBI::dbReadTable(db, "address_cache")), 25)
})

test_that("address_cache only send cache misses to .function", {
  address_processor <- mock(address_result[26:50])
  address_cache(address_stream[26:50], "address_cache", address_processor)
  expect_equal(nrow(mock_args(address_processor)[[1]][[1]]), 25)
})

test_that("address_cache handles fully-cached requests gracefully", {
  address_processor <- mock(address_result[26:50])
  res <- address_cache(address_stream[26:50], "address_cache", address_processor)
  expect_length(mock_args(address_processor),0)
  expect_equal(nrow(res), 25)
})

test_that("address_cache updates the the cache file after cache misses", {
  db <- local_db(sqlite_file)
  expect_equal(nrow(DBI::dbReadTable(db, "address_cache")), 50)
  expect_named(DBI::dbReadTable(db, "address_cache"), c(address_cols, "something", "processed"), ignore.order = TRUE)
})

test_that("address_cache incremental runs match address_cache full runs", {
  address_processor <- function(.) { address_result[.[,..address_cols],on=address_cols]}
  db <- local_db(sqlite_file)
  incremental <- DBI::dbReadTable(db, "address_cache") %>%
    collect() %>%
    setDT()
  full <- address_cache(address_stream[1:50], "address_cache_full", address_processor)

  expect_mapequal(incremental, full)
})

test_that("address_cache updates the the cache file when there are new columns to append", {
  db <- local_db(sqlite_file)
  address_processor <- mock(address_stream[51:100, .(street1, new_int_feature = 1000, new_char_feature = "coolness")])
  address_cache(address_stream, "address_cache", address_processor)
  expect_equal(nrow(mock_args(address_processor)[[1]][[1]]), 50)
  expect_equal(nrow(DBI::dbReadTable(db, "address_cache")), 100)
  expect_named(DBI::dbReadTable(db, "address_cache"), c(address_cols, "something", "processed", "new_int_feature", "new_char_feature"), ignore.order = TRUE)
  expect_equal(DBI::dbReadTable(db, "address_cache")$new_int_feature, c(rep(NA,50),rep(1000,50)))
  expect_equal(DBI::dbReadTable(db, "address_cache")$new_char_feature, c(rep(NA,50),rep("coolness",50)))
})

test_that("address_cache saves only unique addresses", {
  db <- local_db(sqlite_file)
  address_processor <- function(.) { address_result[.,on=address_cols]}
  address_stream <- data.table(street1 = paste(c(5000,5000), "example lane"), something = "extra") %>% rbind(address_stream[0], fill = TRUE)
  address_cache(address_stream, "address_cache", address_processor)
  expect_equal(nrow(DBI::dbReadTable(db, "address_cache")), 101)
})

test_that("address_cache returns data appended to input", {
  db <- local_db(sqlite_file)
  address_processor <- function(.) { address_result[.,on=address_cols]}
  address_stream <- data.table(street1 = paste(c(1,5,1,5), "example lane"), something = "extra") %>% rbind(address_stream[0], fill = TRUE)
  res <- address_cache(address_stream, "address_cache", address_processor) %>% select(street1,processed)
  expect_equal(nrow(DBI::dbReadTable(db, "address_cache")), 101)
  expect_equal(res,cbind(address_stream[,.(street1)],processed = c("1","5","1","5")))
})

test_that("address_cache doesn't copy input data.table", {
  address_processor <- function(.) { address_result[.,on="street1"]}
  address_stream <- data.table(street1 = paste(c(1,5,1,5), "example lane"), something = "extra") %>% rbind(address_stream[0], fill = TRUE)
  tracemem(address_stream)
  expect_silent(address_cache(address_stream, "address_cache", address_processor))
})

test_that("address_cache works with other key columns", {
  data_stream <- data.table(a=seq(100),b=runif(100))
  data_result <- data_stream[,.(a,b=a/2)]
  data_processor <- mock(data_result)
  expect_equal(address_cache(data_stream[100:1], "data_stream", data_processor, key_cols = "a"),data_result[100:1])
  expect_equal(address_cache(data_stream[100:1], "data_stream", data_processor, key_cols = "a"),data_result[100:1])
  expect_length(mock_args(data_processor),1)
})


# address_cache_chunked ---------------------------------------------------

test_that("address_cache_chunked divides data.table into chunks of size n and sends to address_cache", {
  address_cache <- mock(address_stream, cycle = TRUE)
  stub(address_cache_chunked,"address_cache",address_cache)
  address_cache_chunked(address_stream, "address_cache", .function = force, n = 1, parallel = FALSE)
  address_cache_chunked(address_stream, "address_cache", .function = force, n = 10, parallel = FALSE)
  address_cache_chunked(address_stream, "address_cache", .function = force, n = 100, parallel = FALSE)

  expect_length(mock_args(address_cache),111)
  expect_equal(nrow(mock_args(address_cache)[[1]][[1]]),1)
  expect_equal(nrow(mock_args(address_cache)[[101]][[1]]),10)
  expect_equal(nrow(mock_args(address_cache)[[111]][[1]]),100)

})

test_that("address_cache_chunked returns the same results as address_cache for all n and parallel settings", {
  address_stream <- rbind(address_stream, address_stream)
  address_processor <- mock(rbind(address_result, address_result))
  future::plan("multisession")
  withr::defer(future::plan("sequential"))

  expect_equal(address_cache(address_stream, "address_cache", address_processor),
               address_cache_chunked(address_stream, "address_cache", address_processor, n = 1, parallel = FALSE))
  expect_equal(address_cache(address_stream, "address_cache", address_processor),
               address_cache_chunked(address_stream, "address_cache", address_processor, n = 10, parallel = FALSE))
  expect_equal(address_cache(address_stream, "address_cache", address_processor),
               address_cache_chunked(address_stream, "address_cache", address_processor, n = 100, parallel = FALSE))

  expect_equal(address_cache(address_stream, "address_cache", address_processor),
               address_cache_chunked(address_stream, "address_cache", address_processor, n = 1, parallel = TRUE))
  expect_equal(address_cache(address_stream, "address_cache", address_processor),
               address_cache_chunked(address_stream, "address_cache", address_processor, n = 10, parallel = TRUE))
  expect_equal(address_cache(address_stream, "address_cache", address_processor),
               address_cache_chunked(address_stream, "address_cache", address_processor, n = 100, parallel = TRUE))

})


# address_parse -----------------------------------------------------------
address_stream_parsed <- data.table(
  house_number = "30",
  road = "lafayette ave",
  house = "brooklyn academy of music",
  city = "brooklyn",
  state = "ny",
  country = NA_character_,
  po_box = NA_character_,
  unit = NA_character_,
  postcode = "11217"
)

file.remove(sqlite_file)

test_that("address_parse handles cache non-existence and writes a cache file containing only address_cols and libpostal cols", {
  address_stream <- data.table(
    street1 = "Brooklyn Academy of Music",
    street2 = "30 Lafeyette Ave",
    city = "Brooklyn",
    state = "NY",
    postal_code = "11217",
    country = ""
  )

  stub(address_parse_libpostal, "address_exec_libpostal", address_stream_parsed)
  stub(address_parse, "address_parse_libpostal", address_parse_libpostal)

  expect_false(file.exists(sqlite_file))
  address_parse(address_stream)
  expect_true(file.exists(sqlite_file))
  db <- local_db(sqlite_file)
  
  expect_named(DBI::dbReadTable(db, "address_parse"),
    as.character(c(address_cols, paste0("libpostal.", colnames(address_stream_parsed)))),
    ignore.order = TRUE
  )
})



test_that("address_parse only send cache misses to address_parse_libpostal", {
  address_stream <- expand.grid(
    street1 = c("Brooklyn Academy of Music", "BAM"),
    street2 = c("30 Lafeyette Ave", "30 Lafayette"),
    city = c("Brooklyn", "NYC"),
    state = c("NY", "New York"),
    postal_code = c("11217", "00000"),
    country = c("", "US"),
    # should not be a cache miss
    primary_ind = c("Y", "N")
  ) %>%
    lapply(as.character) %>%
    setDT()

  address_stream_parsed <- data.table::rbindlist(rep(list(address_stream_parsed), 63))

  address_exec_libpostal <- mock(address_stream_parsed)
  stub(address_parse_libpostal, "address_exec_libpostal", address_exec_libpostal)
  stub(address_parse, "address_parse_libpostal", address_parse_libpostal)
  address_parse(address_stream)
  expect_equal(length(mock_args(address_exec_libpostal)[[1]][[1]]), 63)
})

test_that("address_parse updates the the cache file after cache misses", {
  db <- local_db(sqlite_file)  
  expect_equal(nrow(DBI::dbReadTable(db, "address_parse")), 64)
  expect_named(DBI::dbReadTable(db, "address_parse"),
    as.character(c(address_cols, paste0("libpostal.", colnames(address_stream_parsed)))),
    ignore.order = TRUE
  )
})

test_that("address_parse incremental runs match address_parse full runs", {
  address_stream <- data.table(
    street1 = c("Brooklyn Academy of Music", "BAM"),
    street2 = "30 Lafeyette Ave",
    city = "Brooklyn",
    state = "NY",
    postal_code = "11217",
    country = ""
  )

  file.remove(sqlite_file)

  stub(address_parse_libpostal, "address_exec_libpostal", address_stream_parsed[, house_number])
  stub(address_parse, "address_parse_libpostal", address_parse_libpostal)

  expect_false(file.exists(sqlite_file))
  address_parse(address_stream[1])

  rm(address_parse_libpostal, address_parse)

  stub(address_parse_libpostal, "address_exec_libpostal", address_stream_parsed)
  stub(address_parse, "address_parse_libpostal", address_parse_libpostal)

  incremental <- address_parse(address_stream)

  rm(address_parse_libpostal, address_parse)
  file.remove(sqlite_file)

  stub(address_parse_libpostal, "address_exec_libpostal", rbind(address_stream_parsed[, house_number], address_stream_parsed, fill = TRUE))
  stub(address_parse, "address_parse_libpostal", address_parse_libpostal)

  full <- address_parse(address_stream)

  expect_mapequal(incremental, full)
})
