library(tidyverse)
library(fs)
library(readxl)
library(rmarkdown)
library(janitor)
library(ckanr)

# Helper functions ==================================

# Writes a CSV file to the "output/" directory, and returns the data if necessary for future piped functions.
write_out_csv <- function(df, filename, na = "") {
  
  df %>%
    write_csv(
      str_c("output/", filename, ".csv"), 
      na = na
      # ,
      # eol = "\r\n",
    )
  
  df
  
}

