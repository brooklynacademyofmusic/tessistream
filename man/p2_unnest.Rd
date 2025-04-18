% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/prospect2_stream.R
\name{p2_unnest}
\alias{p2_unnest}
\title{p2_unnest}
\usage{
p2_unnest(data, colname)
}
\arguments{
\item{data}{data.table}

\item{colname}{character, column to unnest}
}
\value{
unnested data.table, modified in place (unless the column needs to be unnested longer)
}
\description{
Unnest a nested data.table wider. This might be a useful function for
other purposes but will need testing. For now it should at least
work with the nested structures that come from P2 JSONs
}
\note{
Assumes for speed that elements of each column are either all named or all unnamed.
List columns with variable length are filled with NAs during unnesting.
}
