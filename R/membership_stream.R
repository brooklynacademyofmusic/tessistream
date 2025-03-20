

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
#'  outside of the organization if there are none in the organization
#' * `membership_count`: integer, number of memberships a customer has had
#' * `membership_level`: character, the current membership level
#' * `membership_amt`: double, the total value of memberships a customer has had
#' * `membership_start_timestamp_min`, `membership_start_timestamp_max`,
#' `membership_end_timestamp_min`, `membership_end_timestamp_max`: the first/last timestamp of a 
#' membership start/end
#'
#' @return [arrow::Table] of membership data
#' @export
#' @importFrom lubridate parse_date_time
membership_stream <- function() {
  
  m <- stream_from_audit("memberships")
  date_cols <- c("init_dt","expr_dt")
  m[, (date_cols) := lapply(.SD, parse_date_time, 
                            orders = paste0(c("ymd","mdy"),rep(c("","HM","HMS"),each=2))), 
    .SDcols = date_cols]
  
  m <- m[is.na(action) | !grepl("deleted",action,ignore.case=T)]
  # remove the added expiration dates
  m[, memb_level := gsub(" .+","",memb_level)]
  
  setkey(m,cust_memb_no,timestamp)
  
  starts <- m[m[init_dt != lag(init_dt) |
                  event_subtype %in% c("Creation"),
                .I, by="cust_memb_no"]$I,
              .(timestamp = max(timestamp),
                init_dt = max(init_dt)),
              by="cust_memb_no"] %>% 
    .[.(timestamp = pmax(timestamp,init_dt),
        event_subtype = "Start",
        cust_memb_no)]  
  
  ends <- m[m[expr_dt != lag(expr_dt) |
                  event_subtype %in% c("Current"),
                .I, by="cust_memb_no"]$I,
            .(timestamp = max(timestamp),
              expr_dt = max(expr_dt)),
            by="cust_memb_no"] %>%
    .[.(timestamp = pmax(timestamp,expr_dt),
        event_subtype = "End",
        cust_memb_no)]  
  
  
  
  m <- m[event_subtype=="Current",
         .(timestamp, customer_no, group_customer_no, cust_memb_no,
            event_type = "Membership")] %>%
    setleftjoin(starts, by = c("cust_memb_no")) %>% 
    setleftjoin(ends, by = c("cust_memb_no"))
  
  #arrow::as_arrow_table(m)
  
}

#' 
#' @importFrom data.table shift
membership_tree <- function() {
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
  assert_posixct(stream[,column,with=F])
  
  setkeyv(stream,c(by,"timestamp"))
  
  stream[  
    # not the first row of the group
    get(by) == shift(get(by)) & 
    # column has changed
      get(column) != shift(get(column)) & 
    # column isn't effective yet
      column >= timestamp | 
    # or is the first row of the group
      get(by) != shift(get(by)),
    .(timestamp = pmax(timestamp, get(column)))] %>% 
    .[,.(timestamp = max(timestamp)), by = by]
  
  stream
}
