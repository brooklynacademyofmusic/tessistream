

#' membership_stream
#' 
#' Generates a dataset of membership starts, ends and monthly 'controls' that 
#' also keep track of changes to membership level. 
#' Features:
#' * `timestamp`
#' * `customer_no`
#' * `group_customer_no`
#' * `event_type`: Membership
#' * `event_subtype`: Start, End, and Control
#' * `event_subtype2`: New, Renew, Reinstate
#'   * New: first membership ever
#'   * Renew: no lapse between memberships
#'   * Reinstate: membership starts after a lapse
#' * `event_subtype3`: Current or Lapsed
#' * `cust_memb_no`: the membership with the closest start/end
#' * `cust_memb_no_prev`, `cust_memb_no_next`: the previous/next membership in the same organization or 
#'  outside of the organization if there are no more in the organization
#' * `membership_count`: integer, number of memberships a customer has had
#' * `membership_level`: character, the current membership level
#' * `membership_amt`: double, the total value of memberships a customer has had
#' * `membership_start_timestamp_min`, `membership_start_timestamp_max`,
#' `membership_end_timestamp_min`, `membership_end_timestamp_max`: the first/last timestamp of a 
#' membership start/end
#'
#' @param control_period duration after the expiration of a membership to continue adding 
#' control events for analysis
#'
#' @return [arrow::Table] of membership data
#' @export
#' @importFrom lubridate parse_date_time
#' @importFrom data.table frank
membership_stream <- function(control_period = years(4)) {
  
  . <- action <- memb_level <- cust_memb_no <- timestamp <- customer_no <- 
    group_customer_no <- cust_memb_no_next <- cust_memb_no_prev <- memb_amt <- 
    min_timestamp <- max_timestamp <- month <- init_dt <- expr_dt <- event_subtype <- 
    event_type <- NULL
  
  m <- stream_from_audit("memberships")
  date_cols <- c("init_dt","expr_dt")
  m[, (date_cols) := lapply(.SD, parse_date_time, 
                            orders = paste0(c("ymd","mdy"),rep(c("","HM","HMS"),each=2))), 
    .SDcols = date_cols]
  
  m <- m[is.na(action) | !grepl("deleted",action,ignore.case=T)]
  # remove the expiration dates added to level names
  m[, memb_level := gsub(" .+","",memb_level)]
  
  setkey(m,cust_memb_no,timestamp)
  
  # calculate effective starts and ends
  starts <- stream_effective_date(m, "init_dt", "cust_memb_no") %>% 
    .[,.(timestamp, cust_memb_no, event_subtype = "Start")]
  ends <- stream_effective_date(m, "expr_dt", "cust_memb_no") %>%
    .[,.(timestamp, cust_memb_no, event_subtype = "End")]
  
  # build the initial membership stream from starts/ends and tree information
  membership_stream <- rbind(starts,ends,fill=T)[!is.na(timestamp)] 
  membership_tree <- membership_tree()
  setleftjoin(membership_stream,
              membership_tree[,.(cust_memb_no,
                     customer_no, group_customer_no,
                     cust_memb_no_next,cust_memb_no_prev,
                     membership_level = memb_level,
                     memb_amt)],
              by = "cust_memb_no") 
  
  # build membership controls
  controls <- membership_stream[!is.na(timestamp),
                                .(timestamp = seq(min(timestamp),
                                                  max(timestamp)+control_period,
                                                  by="month"),
                                  min_timestamp = min(timestamp),
                                  max_timestamp = max(timestamp),
                                  customer_no = data.table::first(customer_no),
                                  group_customer_no = data.table::first(group_customer_no),
                  event_subtype = "Control"),
                by="cust_memb_no"] %>% 
    # remove controls duplicated by endpoints
    .[floor_date(timestamp,"month") != floor_date(min_timestamp,"month") &
      floor_date(timestamp,"month") != floor_date(max_timestamp,"month")]
  
  # Remove duplicated controls
  controls <- controls[,month := floor_date(timestamp,"month")] %>% 
    .[,I := seq_len(.N),by=list(group_customer_no,month)] %>% 
    .[I == 1] 

  # use initiation times to add membership data to controls  
  controls <- membership_stream[event_subtype=="Start",
                                .(cust_memb_no,cust_memb_no_next,cust_memb_no_prev,
                                 init_dt = timestamp,group_customer_no)] %>% 
    .[controls,on = c("group_customer_no","init_dt" = "timestamp"),
      roll = Inf] %>% 
    .[,`:=`(month = NULL,
            min_timestamp = NULL,
            max_timestamp = NULL,
            timestamp = init_dt,
            init_dt = NULL,
            i.cust_memb_no = NULL)]
  
  # build features by customer
  setkey(membership_stream,group_customer_no,timestamp)
  membership_stream[event_subtype == "Start",`:=`(
    membership_count = cumsum(!duplicated(cust_memb_no)),
    membership_amt = cumsum(memb_amt),
    membership_start_timestamp_min = min(timestamp),
    membership_start_timestamp_max = timestamp
  ),by = "group_customer_no"]
  membership_stream[event_subtype == "End",`:=`(
    membership_end_timestamp_min = min(timestamp),
    membership_end_timestamp_max = timestamp
  ),by = "group_customer_no"]
  membership_stream[,`:=`(
    event_subtype2 = case_when(seq_len(.N) == 1 ~ "New",
                               event_subtype == "Start" & 
                                 timestamp - membership_end_timestamp_max < ddays(1) ~ "Renew",
                               event_subtype == "Start" ~ "Reinstate"),
    event_subtype3 = (cumsum((event_subtype == "Start") - (event_subtype == "End")) > 0) %>% 
      factor(levels = c(T,F), labels = c("Current","Lapsed"))),
  by = "group_customer_no"]
  membership_stream$memb_amt <- NULL
  
  # add in controls
  membership_stream <- rbind(membership_stream,controls,fill=T)
  
  # fill down all features
  setkey(membership_stream,group_customer_no,timestamp)
  setnafill(membership_stream, "locf", 
            cols = c("event_subtype2","event_subtype3",
                     grep("membership",colnames(membership_stream),
                          value = TRUE)),
            by = "group_customer_no")
 
  membership_stream[,event_type := "Membership"]
  
  # write it out
  write_cache(membership_stream, "membership_stream", "stream", overwrite = T)
  
  membership_stream
}

#' 
#' @importFrom data.table shift
membership_tree <- function() {
  group_customer_no <- init_dt <- cust_memb_no <- NULL
  
  m <- read_tessi("memberships") %>% collect %>% setDT
  setkey(m,group_customer_no,init_dt)  
  
  cols <- c("cust_memb_no_prev","cust_memb_no_next")
  
  for(col in cols) {
    n <- ifelse(col == "cust_memb_no_prev", 1, -1)
    m[,(col) := shift(cust_memb_no,n), by = c("group_customer_no","memb_org_no")]
    m[,(col) := coalesce(get(col),shift(cust_memb_no,n)), by = c("group_customer_no")]
  }
  
  m
}

#' stream_effective_date
#' 
#' The effective date/time of a changing date/time column `dt` is the last 
#' change before that change becomes effective, i.e. when `dt == timestamp`.
#'
#' @param stream `data.table` with `timestamp`, and the columns named in `column`
#' and `by`, if defined.
#' @param column `character(1)` column name to make effective, must identify
#' a date or datetime column.
#' @param by `character(1)` optional column name to group rows by
#' @return `data.table` with columns named by `column` and `by`
#' @export
#'
stream_effective_date <- function(stream, column, by = NULL) {
  assert_data_table(stream)
  assert_names(names(stream), must.include = c(column,by,"timestamp"))
  assert_posixct(stream[[column]])
  
  . <- timestamp <- NULL
  
  if(is.null(by)) {
    by = "_stream_effective_date_I"
    stream[,(by) := .I]
  }
  
  setorderv(stream,c(by,"timestamp"))

  out <- stream[stream[,  
    # column has changed
      get(column) != shift(get(column)) & 
    # column isn't effective yet
      get(column) >= timestamp | 
    # or is first in group
      is.na(shift(get(column))), by = by]$V1,
      .(timestamp = max(pmax(timestamp,get(column))),
        column = max(get(column))), 
    by = by] %>% setNames(c(by,"timestamp",column))
  
  if(by == "_stream_effective_date_I") {
    stream[,(by) := NULL]
    out[,(by) := NULL]
  }
  
  out
  
}
