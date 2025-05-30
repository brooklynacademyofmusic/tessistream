withr::local_package("mockery")
withr::local_package("checkmate")



# membership_tree ---------------------------------------------------------

test_that("membership_tree produces a sequence of memberships", {
  n <- 1000
  m <- data.table(cust_memb_no = seq(n),
                  group_customer_no = rep(seq(100),length.out = n),
                  memb_org_no = 1,
                  init_dt = Sys.Date() + seq(n))
  stub(membership_tree, "read_tessi", m)
  
  expected <- data.table(group_customer_no = 1,
                         memb_org_no = 1,
                         cust_memb_no = 1 + seq(0,9)*100)
  expected[,`:=`(
    cust_memb_no_next = c(cust_memb_no[-1],NA),
    cust_memb_no_prev = c(NA,cust_memb_no[-.N]),
    init_dt = Sys.Date() + cust_memb_no
    )]
  
  expect_equal(membership_tree()[group_customer_no==1],expected,
              ignore_attr = "sorted",
              list_as_map = TRUE)
})

test_that("membership_tree produces a sequence of memberships that crosses memb_org boundaries", {
  n <- 1000
  m <- data.table(cust_memb_no = seq(n),
                  group_customer_no = rep(seq(100),length.out = n),
                  memb_org_no = rep(c(1,2,3),length.out = n),
                  init_dt = Sys.Date() + seq(n))
  stub(membership_tree, "read_tessi", m)
  
  expected <- data.table(group_customer_no = 1,
                         memb_org_no = rep(c(1,2,3),length.out = 10),
                         cust_memb_no = 1 + seq(0,9)*100)
  expected[,`:=`(
    cust_memb_no_next = c(cust_memb_no[-1],NA),
    cust_memb_no_prev = c(NA,cust_memb_no[-.N]),
    init_dt = Sys.Date() + cust_memb_no
  ),by="memb_org_no"]
  
  # the first/last of each org goes to the prior/next membership
  expected[c(1,2,3), cust_memb_no_prev := shift(cust_memb_no)]
  expected[c(8,9,10), cust_memb_no_next := shift(cust_memb_no,-1)]
  
  expect_equal(membership_tree()[group_customer_no==1],expected,
               ignore_attr = "sorted",
               list_as_map = TRUE)
})



# stream_effective_date ---------------------------------------------------

test_that("stream_effective_date returns input if target column is always less than or equal to timestamp", {
  test <- data.table(timestamp = sample(seq(as_datetime("2000-01-01"),as_datetime("2100-01-01"),by="hour"),
                                        size = 100,
                                        replace = T)) %>% 
    .[,target_dt := timestamp]

  expect_equal(stream_effective_date(copy(test),"target_dt"),copy(test))
  
  test[,target_dt := timestamp - ddays(1)]
  
  expect_equal(stream_effective_date(copy(test),"target_dt"),copy(test))
  
  test[,target_dt := timestamp + ddays(1)]
  
  expect_failure(expect_equal(stream_effective_date(copy(test),"target_dt"),copy(test)))
})

test_that("stream_effective_date adjusts timestamp if target column is greater than timestamp", {
  test <- data.table(timestamp = sample(seq(as_datetime("2000-01-01"),as_datetime("2100-01-01"),by="hour"),
                                        size = 100,
                                        replace = T)) %>% 
    .[,target_dt := timestamp + ddays(1)]
  
  expect_equal(stream_effective_date(copy(test),"target_dt"),copy(test)[,timestamp := target_dt])
})

test_that("stream_effective_date returns one row per group", {
  test <- data.table(timestamp = sample(seq(as_datetime("2000-01-01"),as_datetime("2100-01-01"),by="hour"),
                                        size = 100,
                                        replace = T)) %>% 
    .[,`:=`(group = sample(letters,size=.N,replace=T),
            target_dt = timestamp - ddays(1))]
  
  expect_equal(stream_effective_date(copy(test),"target_dt","group"),
               setorderv(test[,lapply(.SD,min),by="group"],"group"))
  
  test[,target_dt := timestamp + ddays(1)]
  
  expect_equal(stream_effective_date(copy(test),"target_dt","group"),
               setorderv(test[,.(timestamp = max(target_dt),
                                 target_dt = max(target_dt)),by="group"],"group"))
  
})


# membership_stream -------------------------------------------------------



test_that("membership_stream returns features", {
  stub(membership_stream, "stream_from_audit", 
       readRDS(rprojroot::find_testthat_root_file("membership_stream.Rds")))
  stub(membership_tree, "read_tessi",
       readRDS(rprojroot::find_testthat_root_file("membership_stream-memberships.Rds")))
  stub(membership_stream, "membership_tree", membership_tree)
  stub(membership_stream, "write_cache", NULL)
  
  m <- membership_stream()
  expect_names(names(m),
               permutation.of = c("timestamp", "customer_no", "group_customer_no",
                                  "event_type", "event_subtype", "event_subtype2",
                                  "event_subtype3", "cust_memb_no", "cust_memb_no_prev",
                                  "cust_memb_no_next", 
                                  "membership_count", "membership_level", "membership_amt", 
                                  "membership_start_timestamp_min",
                                  "membership_start_timestamp_max",
                                  "membership_end_timestamp_min",
                                  "membership_end_timestamp_max")
  )

  sometimes <- c("membership_end_timestamp_min","membership_end_timestamp_max")
  rarely <- c("cust_memb_no_prev","cust_memb_no_next")
  cumulative <- setdiff(grepv("membership",colnames(m)),"membership_level")
  
  expect_equal(m[,lapply(.SD,\(.) !any(is.na(.))),.SDcols = setdiff(colnames(m),c(sometimes,rarely))],
               m[,lapply(.SD,\(.) T),.SDcols = setdiff(colnames(m),c(sometimes,rarely))])
  
  expect_equal(m[membership_count>1,lapply(.SD,\(.) !any(is.na(.))),.SDcols = sometimes],
               m[,lapply(.SD,\(.) T),.SDcols = sometimes])
  
  setkey(m,group_customer_no,timestamp)
  expect_equal(m[,lapply(.SD,\(.) .-shift(.)),.SDcols = cumulative,by="group_customer_no"] %>% 
               .[,lapply(.SD,\(.) all(coalesce(as.numeric(.),0)>=0)), .SDcols = cumulative],
               m[,lapply(.SD,\(.) T),.SDcols = cumulative])
  
})

test_that("membership_stream returns one start, one end and multiple controls", {
  stub(membership_stream, "stream_from_audit", 
       readRDS(rprojroot::find_testthat_root_file("membership_stream.Rds")))
  stub(membership_tree, "read_tessi",
       readRDS(rprojroot::find_testthat_root_file("membership_stream-memberships.Rds")))
  stub(membership_stream, "membership_tree", membership_tree)
  stub(membership_stream, "write_cache", NULL)
  
  m <- membership_stream()
  
  expect_equal(nrow(m[event_subtype == "Start"]),m[event_subtype %in% c("Start","End"),dplyr::n_distinct(cust_memb_no)])
  expect_equal(nrow(m[event_subtype == "End"]),m[event_subtype %in% c("Start","End"),dplyr::n_distinct(cust_memb_no)])
  expect_gte(nrow(m[event_subtype == "Control"]),m[,dplyr::n_distinct(cust_memb_no)]*48)
})

test_that("membership_stream has no more than one item per group_customer_no per month (usually!)", {
  stub(membership_stream, "stream_from_audit", 
       readRDS(rprojroot::find_testthat_root_file("membership_stream.Rds")))
  stub(membership_tree, "read_tessi",
       readRDS(rprojroot::find_testthat_root_file("membership_stream-memberships.Rds")))
  stub(membership_stream, "membership_tree", membership_tree)
  stub(membership_stream, "write_cache", NULL)
  
  m <- membership_stream()
  memberships <-  readRDS(rprojroot::find_testthat_root_file("membership_stream-memberships.Rds")) %>% setDT
  
  expect_lte(m[,.N,by=list(group_customer_no,floor_date(timestamp,"month"))][N>1,.N],20)
})

test_that("membership_stream writes out a dataset", {
  stub(membership_stream, "stream_from_audit", 
       readRDS(rprojroot::find_testthat_root_file("membership_stream.Rds")))
  stub(membership_tree, "read_tessi",
       readRDS(rprojroot::find_testthat_root_file("membership_stream-memberships.Rds")))
  stub(membership_stream, "membership_tree", membership_tree)
  write_cache <- mock()
  stub(membership_stream, "write_cache", write_cache)
  
  m <- membership_stream()
  
  expect_length(mock_args(write_cache),1)
  
})
