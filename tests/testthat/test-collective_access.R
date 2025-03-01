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

test_that("collective_access_stream sends queries in batches", {
  ca <- readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds"))
  content <- mock(ca)
  stub(collective_access_search,"POST",NULL)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  .collective_access_search <- collective_access_search("occurrences","*",base_url)
  
  collective_access_search <- mock(.collective_access_search,
                                   .collective_access_search[1:2],
                                   .collective_access_search[3:4],
                                   .collective_access_search[5:6])
  
  stub(collective_access_stream, "collective_access_search", collective_access_search)
  stub(collective_access_stream, "write_cache", NULL)
  
  out <- collective_access_stream("ca_occurrences","collective_access","*",list(),batch_size = 2)
  expect_length(mock_args(collective_access_search),length(ca$results)/2+1)
  purrr::map2(
    mock_args(collective_access_search)[-1] %>% map_chr(2),
    ca$results %>% map_int("id") %>% sort %>% split(rep(1:3,each=2)),
    \(a,e) expect_match(a,paste0("\\[",e[1]," TO ",e[2],"\\]")))

})

test_that("collective_access_stream unlists atomic columns", {
  ca <- readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds"))
  content <- mock(ca)
  stub(collective_access_search,"POST",NULL)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  .collective_access_search <- collective_access_search("occurrences","*",base_url)
  
  collective_access_search <- mock(.collective_access_search,
                                   .collective_access_search[1:2],
                                   .collective_access_search[3:4],
                                   .collective_access_search[5:6])
  
  stub(collective_access_stream, "collective_access_search", collective_access_search)
  stub(collective_access_stream, "write_cache", NULL)
  
  out <- collective_access_stream("ca_occurrences","collective_access","*",list(),batch_size = 2)
  
  atomic_cols <- c("idno","occurrence_id","display_label")
  expect_equal(setkey(out,id),.collective_access_search[, (atomic_cols) := lapply(.SD,unlist), .SDcols = atomic_cols])
  
})

test_that("collective_access_stream parses `features` and passes with defaults to collective_access_search", {
  ca <- readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds"))
  content <- mock(ca)
  stub(collective_access_search,"POST",NULL)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  .collective_access_search <- collective_access_search("occurrences","*",base_url)
  
  collective_access_search <- mock(.collective_access_search,cycle=T)
  stub(collective_access_stream, "collective_access_search", collective_access_search)
  stub(collective_access_stream, "write_cache", NULL)
  
  out <- collective_access_stream("ca_occurrences","collective_access","*",
                           list(
                             "one_bundle" = "display_label",
                             "two_bundles" = list("id","idno" = list("with" = "parameter"))
                           ),
                           batch_size = 100)
  expect_length(mock_args(collective_access_search),2)
  
  defaults <- list('convertCodesToDisplayText'=T, 'returnAsArray'=T)
  expect_equal(mock_args(collective_access_search)[[2]][["bundles"]],
               list("display_label"=defaults,
                    "id"=defaults,
                    "idno"=c("with"="parameter",defaults)))
  
  expect_names(names(out),permutation.of=c("one_bundle","two_bundles","id"))
  expect_equal(nrow(out),nrow(.collective_access_search))
  expect_equal(out$one_bundle,.collective_access_search$display_label)
  expect_equal(out$two_bundles,purrr::map2(.collective_access_search$id,.collective_access_search$idno,c))
})

test_that("collective_access_stream removes blanks/nulls and writes out a dataset", {
  ca <- readRDS(rprojroot::find_testthat_root_file("collective_access-search.Rds"))
  content <- mock(ca)
  stub(collective_access_search,"POST",NULL)
  stub(collective_access_search,"content",content)
  stub(collective_access_search,"collective_access_login","abc123")
  .collective_access_search <- collective_access_search("occurrences","*",base_url)
  .collective_access_search[id==min(unlist(id)),idno:=NA]
  .collective_access_search[id==min(unlist(id)),display_label:=""]
  
  collective_access_search <- mock(.collective_access_search,cycle=T)
  stub(collective_access_stream, "collective_access_search", collective_access_search)
  
  write_cache <- mock()
  stub(collective_access_stream, "write_cache", write_cache)
  
  out <- collective_access_stream("ca_occurrences","collective_access","*",
                                  list(
                                    "one_bundle" = "display_label",
                                    "two_bundles" = list("id","idno" = list("with" = "parameter"))
                                  ),
                                  batch_size = 100)
  expect_length(mock_args(collective_access_search),2)
  
  defaults <- list('convertCodesToDisplayText'=T, 'returnAsArray'=T)
  expect_equal(mock_args(collective_access_search)[[2]][["bundles"]],
               list("display_label"=defaults,
                    "id"=defaults,
                    "idno"=c("with"="parameter",defaults)))
  
  expect_names(names(out),permutation.of=c("one_bundle","two_bundles","id"))
  expect_equal(nrow(out),nrow(.collective_access_search))
  expect_equal(out$one_bundle[1],list(character()))
  expect_equal(out$two_bundles[1],list(as.character(.collective_access_search$id[1])))
  expect_equal(out$one_bundle[2:6],.collective_access_search$display_label[2:6])
  expect_equal(out$two_bundles[2:6],purrr::map2(.collective_access_search$id,.collective_access_search$idno,c)[2:6])
  
  expect_length(mock_args(write_cache),1)
  expect_equal(mock_args(write_cache)[[1]][[1]],out)
})
