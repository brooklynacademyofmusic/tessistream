withr::local_package("checkmate")
withr::local_package("mockery")

suppressWarnings(survey_data <- survey_monkey(rprojroot::find_testthat_root_file("survey_data/Audience_Survey_Spring_2024.xlsx")))
stream_from_audit <- data.table(address=paste0(seq(1000),"@bam.org"),timestamp=Sys.Date(),
                                 customer_no=10000+seq(1000),group_customer_no=100000+seq(1000),primary_ind="Y")

survey_stream_stubbed <- function(email_audit = stream_from_audit, reader = mock(survey_data,cycle=T)) {
  stub(survey_stream,"dir","a")
  stub(survey_stream,"stream_from_audit", email_audit)
  stub(survey_stream,"read_tessi",data.table(customer_no=seq(12000)))
  stub(survey_stream,"write_cache",TRUE)
  stub(survey_stream,"survey_monkey",reader)
  
  survey_stream
}

# survey_find_column ------------------------------------------------------

test_that("survey_find_column identifies the maximum column based on `.f`", {
  survey_stream <- data.table(x=rep(1,100),y=runif(100))
  expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).))
  expect_equal(c("y"=2),survey_find_column(survey_stream,\(.)-.))
})

test_that("survey_find_column warns if more than one column meets the criterion", {
  survey_stream <- data.table(x=rep(1,100),y=runif(100),z=rep(1.1,100))
  expect_equal(c("z"=3),survey_find_column(survey_stream,\(.).,criterion="max"))
  expect_warning(expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).,criterion = 1)))
  
  survey_stream <- data.table(x=rep(1,100),y=runif(100),z=rep(1,100))
  expect_warning(expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).,criterion = 1)))
  expect_warning(expect_equal(c("x"=1),survey_find_column(survey_stream,\(.).,criterion = "max")))
})


# survey_monkey -----------------------------------------------------------

test_that("survey_monkey returns a data.table of survey data", {
  expect_warning(survey_data <- survey_monkey(rprojroot::find_testthat_root_file("survey_data/Audience_Survey_Spring_2024.xlsx")),
    "More than one column found")
  
  expect_data_table(survey_data)
  expect_names(colnames(survey_data),permutation.of=c("email","timestamp","response_id","question","subquestion","answer"))
})

test_that("survey_monkey identifies emails and timestamp columns", {
  expect_warning(survey_data <- survey_monkey(rprojroot::find_testthat_root_file("survey_data/Audience_Survey_Spring_2024.xlsx")),
                 "More than one column found")
  
  expect_class(survey_data$timestamp,"POSIXct")
  expect_true(all(survey_data$timestamp>'2024-01-01'))
  expect_true(all(survey_data$timestamp<'2025-01-01'))
  expect_true(all(grepl("@",survey_data$email)))
})


# survey_stream -----------------------------------------------------------

test_that("survey_stream loads data from `survey_dir`", {
  survey_reader <- mock(survey_data[question != "Customer number"],cycle=T)
  survey_stream <- survey_stream_stubbed(reader = survey_reader)
  survey_stream()
  
  expect_length(mock_args(survey_reader), 1)
  expect_equal(mock_args(survey_reader)[[1]][[1]],"a")
  
  stub(survey_stream,"dir",c("a","b"))
  expect_error(survey_stream(),"duplicate files")
  
})

test_that("survey_stream identifies customers by email address", {
  survey_data <- survey_data[question != "Customer number"]
  survey_stream <- survey_stream_stubbed(reader = mock(survey_data))
  stub(survey_stream,"anonymize",function(.).)
  survey_stream <- survey_stream()
  
  expect_equal(survey_stream[customer_hash %in% stream_from_audit$customer_no,customer_hash],
               survey_data[email %in% stream_from_audit$address,as.integer(gsub("@bam.org","",email))+10000])
})

test_that("survey_stream fills in customer number if it has been collected as a question", {
  survey_stream <- survey_stream_stubbed()
  stub(survey_stream,"anonymize",function(.).)

  expect_warning(survey_stream <- survey_stream(),"Found customer number question.+Customer number")
  expect_equal(survey_stream[!customer_hash %in% stream_from_audit$customer_no,customer_hash],
               survey_data[question == "Customer number"] %>% 
                 .[survey_data[!email %in% stream_from_audit$address & question != "Customer number"],
                   as.numeric(coalesce(answer,i.response_id)),
                   on="email"]
  )
})

test_that("survey_stream anonymizes customer number", {
  expect_warning(survey_stream <- survey_stream_stubbed()(),"Found customer number question.+Customer number")
  
  expect_true(all(nchar(survey_stream$customer_hash)==64))
  expect_true(all(nchar(survey_stream$group_customer_hash)==64))
  expect_failure(expect_contains(survey_stream$question,"Customer number"))
})

test_that("survey_stream returns a data.table", {
  survey_stream <- survey_stream_stubbed()
  stub(survey_stream,"dir",dir)

  expect_warning(survey_stream <- survey_stream(),"Found customer number question.+Customer number")
  
  expect_data_table(survey_stream)
  expect_names(colnames(survey_stream), permutation.of=c("customer_hash","group_customer_hash","timestamp","response_id","survey","question","subquestion","answer","filename"))
  expect_equal(survey_stream$filename[1],rprojroot::find_testthat_root_file("survey_data/Audience_Survey_Spring_2024.xlsx"))
})

test_that("survey_stream writes to a cache", {
  tessilake::local_cache_dirs()
  withr::local_package("dplyr")
  
  survey_data_split <- split(survey_data,survey_data$timestamp > median(survey_data$timestamp))
  
  survey_stream <- survey_stream_stubbed(reader = mock(survey_data_split[[1]],survey_data_split[[2]]))
  stub(survey_stream,"dir",dir)
  stub(survey_stream,"write_cache",write_cache)
  
  expect_warning(survey_stream(),"Found customer number question.+Customer number")
  expect_equal(read_cache("survey_stream","stream") %>% tally %>% collect %>% as.integer,
               nrow(survey_data_split[[1]][question != "Customer number"]))

  expect_warning(survey_stream(),"Found customer number question.+Customer number")
  expect_equal(read_cache("survey_stream","stream") %>% tally %>% collect %>% as.integer,
               nrow(survey_data[question != "Customer number"]))

  
})

# survey_cross ------------------------------------------------------------

test_that("survey_cross extracts question info", {
  survey_stream <- survey_stream_stubbed()
  stub(survey_stream,"dir",dir)
  expect_warning(survey_stream <- survey_stream(),"Found customer number question.+Customer number")
  
  survey_data <- survey_cross(survey_stream,"year","income")
  expect_names(colnames(survey_data),must.include = paste(c("question","answer","subquestion"),c("1","2"),sep="."))
  expect_equal(survey_data$question.1[1], "In what year were you born?")
  expect_equal(survey_data$question.2[1], "What is your annual household income?")
})


# survey_append_tessi -----------------------------------------------------

test_that("survey_append_tessi appends data from tessitura", {
  survey_stream <- survey_stream_stubbed()
  stub(survey_stream,"dir",dir)
  expect_warning(survey_stream <- survey_stream(),"Found customer number question.+Customer number")
  
  stub(survey_append_tessi,"read_tessi", data.table(customer_no = 10003, group_customer_no=100003, cont_amt = seq(100)))
  survey_stream <- survey_append_tessi(survey_stream, "contributions", cont_amt = sum(cont_amt,na.rm=T))
  expect_names(colnames(survey_stream), must.include = "cont_amt")
  expect_equal(survey_stream[!is.na(cont_amt),cont_amt][1],50*101)
  
})
