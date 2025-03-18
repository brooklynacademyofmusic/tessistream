withr::local_package("mockery")
withr::local_package("checkmate")

test_that("membership_stream returns features", {
  stub(membership_stream, "stream_from_audit", 
       readRDS(rprojroot::find_testthat_root_file("membership_stream.Rds")))

  expect_names(names(membership_stream()),
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
})


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
