
<!-- README.md is generated from README.Rmd. Please edit that file -->

# tessistream

<!-- badges: start -->

[![R-CMD-check](https://github.com/brooklynacademyofmusic/tessistream/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/brooklynacademyofmusic/tessistream/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/brooklynacademyofmusic/tessistream/branch/main/graph/badge.svg?token=3R8UJNG6QY)](https://app.codecov.io/gh/brooklynacademyofmusic/tessistream?branch=main)
<!-- badges: end -->

Builds timestamped datasets based on contributions, ticket sales,
address changes, inventory for historical and predictive analysis.

- [address_stream](NA) - customer address cleaning plus appends from US
  Census and iWave

- TODO: `benefit_stream` - discounting and other benefits

- [contribution_stream](NA) - donations and other contributions

- [email_stream](NA) - email sends, opens, and clicks

- [p2_stream](NA) - email sends, opens, and clicks from the P2 API

- TODO: `inventory_stream` - number of tickets available for sale and
  hold code analysis

- [membership_stream](NA) - membership starts, ends, and value

- TODO: `ticket_future_stream` - prediction of future ticket purchases
  based on inventory and past buying

- TODO: `ticket_stream` - ticket purchases including discounting
  information

- [stream](NA) - union of the above streams for analysis

## Installation

You can install the development version of tessistream from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("brooklynacademyofmusic/tessistream")

# to install libpostal, run in the RStudio terminal:
scripts/install_libpostal.sh
```

## Example

``` r
library(tessistream)
## basic example code
```
