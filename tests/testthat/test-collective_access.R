withr::local_package("mockery")
withr::local_package("checkmate")

base_url <- "http://collective.access"
login <- "me:pa$$word"
# collective_access_login -------------------------------------------------

test_that("collective_access_login sends username/password and gets a token from collective access",{
  GET <- mock()
  content <- mock(list(ok=T,authToken="abc123"))
  stub(collective_access_login,"GET",GET)
  stub(collective_access_login,"content",content)
  
  expect_equal(collective_access_login(login,base_url),"abc123")
  expect_length(mock_args(GET),1)
  expect_equal(mock_args(GET)[[1]]$username,"me")
  expect_equal(mock_args(GET)[[1]]$password,"pa$$word")
})

test_that("collective_access_login throws an error on authentication failure",{
  GET <- mock()
  content <- mock(NULL)
  stub(collective_access_login,"GET",GET)
  stub(collective_access_login,"content",content)
  
  expect_error(collective_access_login(login,base_url),"Login.+failed")
  expect_length(mock_args(GET),1)
  expect_equal(mock_args(GET)[[1]]$username,"me")
  expect_equal(mock_args(GET)[[1]]$password,"pa$$word")
})

# collective_access_search ------------------------------------------------

test_that("collective_access_search queries the database",{
  withr::local_package("purrr")
  POST <- mock()
  content <- mock(readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds")) %>% modify_at("results",map,discard_at,c("ca_entities","venue")))
  stub(collective_access_search,"POST",POST)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  
  res <- collective_access_search("ca_occurrences","*",base_url,login)
  expect_equal(nrow(res),6)
  expect_names(colnames(res),permutation.of=c('occurrence_id','id','idno','display_label'))
})

test_that("collective_access_search returns named bundles",{
  withr::local_package("purrr")
  POST <- mock()
  content <- mock(readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds")) %>% modify_at("results",map,discard_at,"ca_entities"))
  stub(collective_access_search,"POST",POST)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  
  res <- collective_access_search("ca_occurrences","*",base_url,login)
  expect_equal(nrow(res),6)
  expect_names(colnames(res),permutation.of=c('occurrence_id','id','idno','display_label','venue'))
  expect_list(res$venue,types=c("character","null"))
})

test_that("collective_access_search returns named bundles and flattens list elements",{
  withr::local_package("purrr")
  POST <- mock()
  content <- mock(readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds")))
  stub(collective_access_search,"POST",POST)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  
  res <- collective_access_search("ca_occurrences","*",base_url,login)
  expect_equal(nrow(res),6)
  expect_names(colnames(res),permutation.of=c('occurrence_id','id','idno','display_label','venue','ca_entities'))
  expect_list(res$ca_entities,types=c("character","null"))
})

# collective_access_c -----------------------------------------------------

test_that("collective_access_c combines elements, stripping out NULLs, NAs, and blanks", {
  a <- c("",letters,NA,"null","NULL"," ")
  b <- c(list(NULL,NA),list(list(list(NULL),""),"null"),LETTERS,1,2,3)
  
  expect_equal(collective_access_c(a,b),c(letters,LETTERS,1,2,3))
  expect_equal(collective_access_c(b,a),c(LETTERS,1,2,3,letters))
})

test_that("collective_access_c returns a vector of the same type as the input", {
  a <- seq(100)
  b <- Sys.Date()
  
  expect_equal(collective_access_c(a,b),c(seq(100),Sys.Date()))
})

# collective_access_stream ------------------------------------------------


