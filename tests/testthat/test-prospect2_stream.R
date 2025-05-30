withr::local_package("mockery")
withr::defer(p2_db_close())
withr::local_envvar(R_CONFIG_FILE = "tessistream-config.yml")
future::plan(future::sequential)


# p2_query_table_length ---------------------------------------------------

test_that("p2_query_table_length queries url to get total", {
  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_table_length, "GET", GET)
  stub(p2_query_table_length, "content", I)

  expect_equal(p2_query_table_length("test"),1234)
  expect_match(mock_args(GET)[[1]][[1]], "limit=1")
})

test_that("p2_query_table_length handles missing meta by querying once", {
  GET <- mock(list(test = seq(1234)), cycle = T)
  stub(p2_query_table_length, "GET", GET)
  stub(p2_query_table_length, "content", I)

  expect_equal(p2_query_table_length("test"),1234)
  expect_match(mock_args(GET)[[1]][[1]], "limit=1")
})


# p2_query_api ------------------------------------------------------------


test_that("p2_query_api successfully connects to p2", {
  stub(p2_query_api, "content",function(.){rlang::abort("content",result=.)})
  stub(p2_query_api, "p2_query_table_length", 1)
  stub(p2_query_api, "make_resilient", force)

  result <- tryCatch(p2_query_api(file.path(api_url,"api/3/contacts")),error=function(e){e$parent$result})

  expect_length(content(result),3)
  expect_length(content(result)$contacts,1)
})


test_that("p2_query_api uses offset to change the data loaded", {
  GET <- mock(list(test = seq(1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)
  p2_query_api("test", offset = 1232)

  expect_match(mock_args(GET)[[1]][[1]], "limit=2")
  expect_match(mock_args(GET)[[1]][[1]], "offset=1232")

  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "p2_query_table_length", 1234)
  p2_query_api("test", offset = 1233)
  expect_match(mock_args(GET)[[1]][[1]], "limit=1")
  expect_match(mock_args(GET)[[1]][[1]], "offset=1233")
})

test_that("p2_query_api returns NULL when offset is larger than available data", {
  GET <- mock(list(test = seq(1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)
  expect_equal(p2_query_api("test", offset = 1234),NULL)
  expect_length(mock_args(GET),0)

  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "GET", GET)
  expect_equal(p2_query_api("test", offset = 1235),NULL)
  expect_length(mock_args(GET),0)
})

test_that("p2_query_api queries url in groups of 100", {
  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)
  p2_query_api("test")

  expect_length(mock_args(GET), 13)
  # offsets increase by 100
  imap(mock_args(GET), ~ expect_match(.x[[1]], paste0("offset=", (.y - 1) * 100)))
  # all but the last one has limit 100
  expect_match(purrr::map_chr(head(mock_args(GET), -1), 1), "limit=100")
})

test_that("p2_query_api queries url starting from offset in groups of 100", {
  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)
  p2_query_api("test", offset = 1)

  expect_length(mock_args(GET), 13)
  # offsets start at 1 and increase by 100
  imap(mock_args(GET), ~ expect_match(.x[[1]], paste0("offset=", (.y - 1) * 100 + 1)))
  # all but the last one has limit 100
  expect_match(purrr::map_chr(head(mock_args(GET), -1), 1), "limit=100")
  # last one is one less
  expect_match(tail(mock_args(GET), 1)[[1]][[1]], "limit=33")
})

test_that("p2_query_api queries url starting from offset in groups of 100 and stops when max_len rows have been queried", {
  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)

  p2_query_api("test", max_len = 100)
  expect_length(mock_args(GET), 1)

  p2_query_api("test", offset = 1, max_len = 200)
  expect_length(mock_args(GET), 3)

  p2_query_api("test", offset = 1000, max_len = 300)
  expect_length(mock_args(GET), 6)

  purrr::map2(purrr::map(mock_args(GET),1),
              c(0,1,101,1000,1100,1200),
             ~expect_match(.x, paste0("offset=", .y)))
  # all but the last one has limit 100
  expect_match(purrr::map_chr(head(mock_args(GET), -1), 1), "limit=100")
  # last one is 34
  expect_match(tail(mock_args(GET), 1)[[1]][[1]], "limit=34")
})

test_that("p2_query_api will run specific jobs when specified", {
  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)

  jobs <- data.table(off = as.integer(runif(100,0,10000)),
                     len = as.integer(runif(100,0,10000)))

  p2_query_api("test", jobs = jobs)
  expect_length(mock_args(GET), 100)

  purrr::map2(purrr::map(mock_args(GET),1),
              jobs$off,
              ~expect_match(.x, paste0("offset=", .y)))
  purrr::map2(purrr::map(mock_args(GET),1),
              jobs$len,
              ~expect_match(.x, paste0("limit=", .y)))

})

test_that("p2_query_api complains when jobs isn't valid or offset/max_len are set", {
  GET <- mock(list("meta" = list("total" = 1234)), cycle = T)
  stub(p2_query_api, "p2_query_table_length", 1234)
  stub(p2_query_api, "GET", GET)
  stub(p2_query_api, "content", I)
  stub(p2_query_api, "p2_combine_jsons", I)

  jobs <- data.table(off = as.integer(runif(100,0,10000)),
                     len = as.integer(runif(100,0,10000)))

  expect_error(p2_query_api("test", jobs = jobs[,off]), "Must be.+data.frame")
  expect_error(p2_query_api("test", jobs = jobs[,.(off)]), "missing.+len")
  expect_error(p2_query_api("test", jobs = jobs[,.(len)]), "missing.+off")

  expect_warning(p2_query_api("test", jobs = jobs, offset = 1), "ignoring.+offset")
  expect_warning(p2_query_api("test", jobs = jobs, max_len = 1), "ignoring.+max_len")

})

# p2_json_to_datatable --------------------------------------------------------

test_that("p2_json_to_datatable appends jsons into a df", {
  test_json <- rep(list(list(name = "test", value = "a thing")), 100)
  combined <- p2_json_to_datatable(test_json)

  expect_equal(nrow(combined), 100)
  expect_named(combined, c("name", "value"))
})

test_that("p2_json_to_datatable creates list columns", {
  test_json <- rep(list(list(name = "test", value = "a thing", obj = list(name = "nest", value = "nested thing"))), 100)
  combined <- p2_json_to_datatable(test_json)

  expect_equal(nrow(combined), 100)
  expect_named(combined, c("name", "value", "obj"))
  expect_equal(map(combined$obj, names), rep(list(c("name", "value")), 100))
})

test_that("p2_json_to_datatable handles non-list types", {
  test_json <- rep(list(list(name = "test", value = "a thing", weird = sys.call(), weirder = attributes(data.table())$.internal.selfref)), 100)
  combined <- p2_json_to_datatable(test_json)

  expect_equal(nrow(combined), 100)
  expect_named(combined, c("name", "value", "weird", "weirder"))
})

# p2_combine_jsons --------------------------------------------------------

test_that("p2_combine_jsons binds dataframes", {
  test_dfs <- rep(list(list(df = data.frame(a = seq(50), b = seq(0, 99, by = 2)))), 100)
  combined <- p2_combine_jsons(test_dfs)
  expect_equal(names(combined), "df")
  expect_equal(nrow(combined$df), 5000)
})

test_that("p2_combine_jsons merges names", {
  test_dfs <- rep(list(list(df = data.frame(a = seq(50), b = seq(0, 99, by = 2)))), 100)
  test_dfs2 <- rep(list(list(df2 = data.frame(a = seq(50), b = seq(0, 99, by = 2)))), 10)
  combined <- p2_combine_jsons(c(test_dfs, test_dfs2))
  expect_equal(names(combined), c("df", "df2"))
  expect_equal(map_int(combined, nrow), c("df" = 5000, "df2" = 500))
})

# p2_db --------------------------------------------------------------

test_that("p2_db_open complains if the cache path doesn't exist", {
  expect_error(p2_db_open(), "Please set.+cache path")
  expect_equal(tessistream$p2_db, NULL)
})

tessilake::local_cache_dirs()
withr::defer(p2_db_close())
test_that("p2_db_open creates a database if one doesn't exist", {
  expect_warning(p2_db_open(), "Creating path")
  expect_s4_class(tessistream$p2_db, "SQLiteConnection")
})

test_that("p2_db_update creates a table if one doesn't exist", {
  df <- data.table(id = seq(100))
  p2_db_update(df, "test_create")
  expect_true("test_create" %in% DBI::dbListTables(tessistream$p2_db))
  expect_equal(dplyr::tbl(tessistream$p2_db, "test_create") %>% as.data.table(), df)
})

test_that("p2_db_update does an upsert", {
  df <- data.table(id = seq(100), a = runif(100))
  df2 <- data.table(id = seq(50, 199), a = runif(150))

  p2_db_update(df, "test_upsert")
  p2_db_update(df2, "test_upsert")

  df <- rbind(df[1:49, ], df2)

  expect_equal(dplyr::tbl(tessistream$p2_db, "test_upsert") %>% as.data.table(), df)
})

test_that("p2_db_update overwrites when overwrite = TRUE", {
  df <- data.table(id = seq(50, 199), a = runif(150))

  p2_db_update(df, "test_upsert", overwrite = TRUE)

  expect_equal(dplyr::tbl(tessistream$p2_db, "test_upsert") %>% as.data.table(), df)
})

test_that("p2_db_close closes the database connection", {
  p2_db_close()
  expect_equal(tessistream$p2_db, NULL)
})

# p2_unnest ------------------------------------------------------------

withr::local_package("data.table")

test_that("p2_unnest flattens non-list columns to vectors", {
  dt <- data.table(a = as.list(rep("a", 100)), b = as.list(rep(1, 100)))
  expect_equal(p2_unnest(dt, "a"), data.table(a = rep("a", 100), b = as.list(rep(1, 100))))
  expect_equal(p2_unnest(dt, "b"), data.table(a = rep("a", 100), b = rep(1, 100)))
})

test_that("p2_unnest flattens non-list columns to vectors in place", {
  dt <- data.table(a = as.list(rep("a", 100)), b = as.list(rep(1, 100)))
  tracemem(dt)
  expect_silent(p2_unnest(dt, "a"))
})

test_that("p2_unnest flattens non-list columns with missing elements to vectors with NAs", {
  dt <- data.table(a = as.list(rep(list("a", NULL), 100)), b = as.list(rep(list(1, NULL), 100)))
  dt_copy <- copy(dt)
  expect_equal(p2_unnest(dt_copy, "a"), data.table(a = rep(c("a", NA), 100), b = as.list(rep(list(1, NULL), 100))))
  expect_equal(p2_unnest(dt_copy, "b"), data.table(a = rep(c("a", NA), 100), b = rep(c(1, NA), 100)))
  expect_equal(nrow(dt_copy), nrow(dt))
})

test_that("p2_unnest unnests list columns with names wider", {
  dt <- data.table(a = rep(list(list(a = "a", b = 1)), 100))
  expect_equal(p2_unnest(dt, "a"), data.table(a.a = rep("a", 100), a.b = rep(1, 100)))
})

test_that("p2_unnest unnests list columns without names wider", {
  dt <- data.table(a = rep(list(list("a", 1)), 100))
  expect_equal(p2_unnest(dt, "a"), data.table(a.1 = rep("a", 100), a.2 = rep(1, 100)))
  dt <- data.table(a = rep(list(list("a", 1), list("b")), 100))
  expect_equal(p2_unnest(dt, "a"), data.table(a.1 = rep(c("a", "b"), 100),
                                              a.2 = rep(c(1, NA), 100)))
})

test_that("p2_unnest unnests list columns with missing elements", {
  dt <- data.table(a = rep(list(list(a = "a", b = 1),list(NULL)), 100))
  expect_equal(p2_unnest(dt, "a"), data.table(a.a = rep(c("a",NA), 100),
                                              a.b = rep(c(1,NA), 100)))
  dt <- data.table(a = rep(list(list("a", 1),list(NULL)), 100))
  expect_warning(
    expect_equal(p2_unnest(dt, "a"), data.table(a.1 = rep(c("a",NA), 100),
                                                a.2 = rep(c(1,NA), 100))),
    "has been filled with NA"
  )
  dt <- data.table(a = rep(list(list("a", 1),list(NA)), 100))
  expect_equal(p2_unnest(dt, "a"), data.table(a.1 = rep(c("a",NA), 100),
                                              a.2 = rep(c(1,NA), 100)))

})

test_that("p2_unnest unnests list columns wider in place", {
  dt <- data.table(a = rep(list(list(a = "a", b = 1)), 100))
  tracemem(dt)
  expect_silent(p2_unnest(dt, "a"))
})

test_that("p2_unnest unnests list columns wider and replaces missing elements with NAs", {
  dt <- data.table(a = rep(list(list(a = "a", b = 1), list(a = "a")), 100))
  dt_copy <- copy(dt)
  expect_equal(p2_unnest(dt_copy, "a"), data.table(a.a = rep(c("a", "a"), 100), a.b = rep(c(1, NA), 100)))
  expect_equal(nrow(dt_copy), nrow(dt))
})

# test_that("p2_unnest unnests list columns without names longer", {
#   dt <- data.table(a = rep(list(as.list(seq(100))), 100))
#   expect_equal(p2_unnest(dt, "a"), data.table(a = rep(seq(1, 100), 100)))
# })
#
# test_that("p2_unnest unnests list columns without names longer in place", {
#   dt <- data.table(a = rep(list(as.list(seq(100))), 100))
#   tracemem(dt)
#   expect_silent(p2_unnest(dt, "a"))
# })
#
# test_that("p2_unnest unnests list columns without names longer and replace missing elements with NAs", {
#   dt <- data.table(a = rep(list(c(seq(100), list(NULL)), NULL), 100))
#   dt[, I := .I]
#   expect_mapequal(p2_unnest(dt, "a"), data.table(
#     I = Vectorize(rep.int)(seq(200), c(101, 1)) %>% unlist(),
#     a = rep(c(seq(100), NA, NA), 100)
#   ))
# })

# p2_load -----------------------------------------------------------------

test_that("p2_load dispatches arguments to modify_url", {
  modify_url <- mock(cycle = TRUE)
  stub(p2_load, "modify_url", modify_url)
  stub(p2_load, "p2_query_api", function(url, ...) {
    url;
    list(test=data.table())
  })

  p2_load("test")
  expect_equal(mock_args(modify_url)[[1]][["path"]], "api/3/test")
  p2_load("test", path = "something", query = list("else"))
  expect_equal(mock_args(modify_url)[[2]][["path"]], "something")
  expect_equal(mock_args(modify_url)[[2]][["query"]], list("else"))
})

test_that("p2_load dispatches arguments to p2_query_api", {
  p2_query_api <- mock(list(test = data.table()), cycle = TRUE)
  stub(p2_load, "p2_query_api", p2_query_api)

  p2_load("test", offset = 1234)
  expect_equal(mock_args(p2_query_api)[[1]][[1]], modify_url(api_url, path = "api/3/test"))
  expect_equal(mock_args(p2_query_api)[[1]][["offset"]], 1234)
})

test_that("p2_load dispatches writes to p2_db_update", {
  data <- data.table(id = seq(100))
  obj <- list(test = data)

  p2_query_api <- mock(obj)
  p2_db_update <- mock(cycle = TRUE)
  stub(p2_load, "p2_query_api", p2_query_api)
  stub(p2_load, "p2_db_update", p2_db_update)

  p2_load("test")

  expect_equal(mock_args(p2_db_update)[[1]][[1]], data.table(id = seq(100)))
  expect_equal(mock_args(p2_db_update)[[1]][[2]], "test")
})

test_that("p2_load passes overwrite on to p2_db_update", {
  data <- data.table(id = seq(100))
  obj <- list(test = data)

  p2_query_api <- mock(obj, cycle = TRUE)
  p2_db_update <- mock(cycle = TRUE)
  stub(p2_load, "p2_query_api", p2_query_api)
  stub(p2_load, "p2_db_update", p2_db_update)

  p2_load("test", overwrite = T)
  p2_load("test")

  expect_equal(mock_args(p2_db_update)[[1]][["overwrite"]], T)
  expect_equal(mock_args(p2_db_update)[[2]][["overwrite"]], F)

})

# p2_update ---------------------------------------------------------------

test_that("p2_update loads campaigns, messages, links, bounceLogs, contactLists, customer_nos", {
  p2_load <- mock(cycle = T)
  stub(p2_update, "p2_load", p2_load)
  stub(p2_update, "tbl", data.table(id = 1))

  p2_update()

  expect_true(all(c(
    "campaigns", "messages", "links", "bounceLogs", "contactLists", "fieldValues",
    "logs", "contacts", "links"
  ) %in% purrr::map_chr(mock_args(p2_load), 1)))
})

test_that("p2_update only loads contacts after max(updated_timestamp)", {
  withr::local_package("lubridate")
  p2_load <- mock(cycle = T)
  stub(p2_update, "p2_load", p2_load)
  p2_db_open()
  copy_to(tessistream$p2_db, name = "contacts", data.table(updated_timestamp = seq(today(), today() + ddays(30), by = "day") %>% as.character()))
  copy_to(tessistream$p2_db, name = "logs", data.table(id = seq(100)))
  copy_to(tessistream$p2_db, name = "linkData", data.table(id = seq(100)))
  copy_to(tessistream$p2_db, name = "mppLinkData", data.table(id = seq(100)))

  p2_update()

  call <- mock_args(p2_load) %>% keep(~ .[[1]] == "contacts")
  expect_equal(call[[1]][["query"]], list("filters[updated_after]" = as.character(today() + ddays(30))))
})

test_that("p2_update only loads logs, linkData, and mppLinkData after max(id)", {
  p2_load <- mock(cycle = T)
  stub(p2_update, "p2_load", p2_load)
  p2_db_open()
  
  purrr::imap(c("logs","linkData","mppLinkData"), \(table, i) {
    copy_to(tessistream$p2_db, name = table, data.table(id = seq(100+i) %>% as.character()))
  })
  
  p2_update()

  purrr::imap(c("logs","linkData","mppLinkData"), \(table, i) {
    calls <- mock_args(p2_load) %>% keep(~ .[[1]] %in% c(table))
    expect_equal(calls[[1]][["offset"]], 100+i)
  })
})


# p2_email_map ------------------------------------------------------------

test_that("p2_email_map matches up Tessi customer numbers and emails from within P2 data",{
  withr::local_package("dplyr")

  # duplicated emails in Tessi - 1@gmail.com -> customer 1,51
  tessi <- data.frame(customer_no=seq(100),address=paste0(seq(50),"@gmail.com"),primary_ind='Y')
  tessi_customer_no_map <- data.frame(customer_no=seq(100),group_customer_no=seq(100)+1000)

  # duplicated customer numbers in P2 - customer 1 -> 1@gmail.com, 51@gmail.com
  contacts <- data.frame(id=seq(100),email=paste0(seq(100),"@gmail.com"))
  customer_nos <- data.frame(contact=seq(100),value=seq(50))

  stub(p2_email_map,"tessilake::read_tessi",tessi)
  stub(p2_email_map,"tessilake::tessi_customer_no_map",tessi_customer_no_map)
  stub(p2_email_map,"tbl",mock(contacts,customer_nos, cycle = T))

  expected_mapping <- rbind(
    data.table(id=seq(50),customer_no=seq(100)), # from Tessi
    data.table(id=seq(51,100),customer_no=seq(50)) # from P2 only
  ) %>% .[,`:=`(group_customer_no=customer_no+1000,
                # keeps the email from P2 rather than Tessi
                email=paste0(id,"@gmail.com"))]

  expect_mapequal(p2_email_map() %>% setorderv(c("id","customer_no")),expected_mapping %>% setorderv(c("id","customer_no")))
  expect_equal(p2_email_map()[is.na(group_customer_no),.N],0)
  expect_equal(p2_email_map()[is.na(customer_no),.N],0)
})

test_that("p2_email_map includes P2 emails that don't exist in Tessitura",{
  withr::local_package("dplyr")
  
  # duplicated emails in Tessi - 1@gmail.com -> customer 1,51
  tessi <- data.frame(customer_no=seq(100),address=paste0(seq(50),"@gmail.com"),primary_ind='Y')
  tessi_customer_no_map <- data.frame(customer_no=seq(100),group_customer_no=seq(100)+1000)
  
  # duplicated customer numbers in P2 - customer 1 -> 1@gmail.com, 51@gmail.com
  contacts <- data.frame(id=seq(100),email=paste0(seq(100),"@gmail.com"))
  customer_nos <- data.frame(contact=seq(100),value=seq(50))
  
  # non-Tessi customers
  contacts <- rbind(contacts,
                    data.frame(id=seq(101,200),email=paste0(seq(101,200),"@gmail.com")))
  
  stub(p2_email_map,"tessilake::read_tessi",tessi)
  stub(p2_email_map,"tessilake::tessi_customer_no_map",tessi_customer_no_map)
  stub(p2_email_map,"tbl",mock(contacts,customer_nos, cycle = T))
  
  expected_mapping <- rbind(
    data.table(id=seq(50),customer_no=seq(100)), # from Tessi
    data.table(id=seq(51,200),customer_no=c(seq(50),rep(NA,100))) # from P2
  ) %>% .[,`:=`(group_customer_no=customer_no+1000,
                # keeps the email from P2 rather than Tessi
                email=paste0(id,"@gmail.com"))]
  
  expect_mapequal(p2_email_map() %>% setorderv(c("id","customer_no")),expected_mapping %>% setorderv(c("id","customer_no")))
  expect_equal(p2_email_map()[is.na(group_customer_no),.N],100)
  expect_equal(p2_email_map()[is.na(customer_no),.N],100)
})

p2_db_close()

# p2_stream_build ---------------------------------------------------------------


test_that("p2_stream_build combines log (sends), linkData (opens/clicks), contactLists (unsubs/bounces) and bounceLogs",{
  p2_db_open("p2.sqlite")
  stub(p2_stream_build,"p2_email_map",data.table(id=seq(100),customer_no=seq(100),email=paste0(seq(100),"@gmail.com")))

  p2_stream <- p2_stream_build()
  expect_equal(length(unique(p2_stream$event_subtype)),6)

  expect_equal(p2_stream[is.na(subscriberid),.N],0)

  expect_equal(p2_stream[event_subtype=="Send" & (is.na(campaignid) | is.na(messageid)),.N],0)
  expect_equal(p2_stream[event_subtype=="Open" & (is.na(campaignid) | is.na(messageid) | is.na(linkid)),.N],0)
  expect_equal(p2_stream[event_subtype=="Click" & (is.na(campaignid) | is.na(messageid) | is.na(linkid)),.N],0)
  # only unsubscribes and bounces can be without campaignid
  expect_equal(p2_stream[event_subtype=="Hard Bounce" & (is.na(listid)),.N],0)
  expect_equal(p2_stream[event_subtype=="Unsubscribe" & (is.na(listid)),.N],0)

})

test_that("p2_stream_build outputs a stream-compliant dataset",{
  p2_db_open("p2.sqlite")
  stub(p2_stream_build,"p2_email_map",data.table(id=seq(1e6)) %>%
         .[,`:=`(customer_no = id,
                 group_customer_no = id,
                 email=paste0(seq(id),"@gmail.com"))])

  p2_stream <- p2_stream_build()
  expect_equal(p2_stream[is.na(timestamp),.N],0)
  expect_equal(p2_stream[is.na(group_customer_no),.N],0)

  expect_true(all(c("group_customer_no","customer_no","timestamp","event_type","event_subtype",
                           "campaignid","messageid","linkid","listid") %in% names(p2_stream)))

})

test_that("p2_stream_enrich doesn't grow the file but adds new columns",{
  stub(p2_stream_build,"p2_email_map",data.table(id=seq(100),customer_no=seq(100),email=paste0(seq(100),"@gmail.com")))

  p2_db_open("p2.sqlite")
  p2_stream <- p2_stream_build()
  p2_stream_nrow <- nrow(p2_stream)

  p2_db_open("p2.sqlite")
  p2_stream_enriched <- p2_stream_enrich(p2_stream)

  expect_equal(nrow(p2_stream_enriched),p2_stream_nrow)
  expect_true(all(c("campaign_name","message_name","link_name","list_name") %in% names(p2_stream_enriched)))
})

# p2_stream ---------------------------------------------------------------

test_that("p2_stream writes out two cache files and copies the database", {
  withr::local_package("checkmate")
  expect_file_exists(tessilake::cache_primary_path("p2.sqlite", "stream"))
  expect_false(file.exists(tessilake::cache_path("p2_stream.parquet", "deep", "stream")))
  expect_false(file.exists(tessilake::cache_path("p2_stream_enriched.parquet", "deep", "stream")))

  p2_db_open("p2.sqlite")
  stub(p2_stream_build,"p2_db_close",NULL)
  stub(p2_stream_build,"p2_email_map",data.table(id=seq(100),customer_no=seq(100),email=paste0(seq(100),"@gmail.com")))
  stub(p2_stream,"p2_stream_build",p2_stream_build)
  p2_stream <- p2_stream()

  expect_file_exists(tessilake::cache_path("p2.sqlite", "shallow", "stream"))
  expect_file_exists(tessilake::cache_path("p2.sqlite", "deep", "stream"))
  expect_file_exists(tessilake::cache_path("p2_stream.parquet", "deep", "stream"))
  expect_file_exists(tessilake::cache_path("p2_stream_enriched.parquet", "deep", "stream"))

})

