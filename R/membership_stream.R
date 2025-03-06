

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
#'
membership_stream <- function() {
  
}

