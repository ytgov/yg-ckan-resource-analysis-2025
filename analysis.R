library(tidyverse)

packages <- read_csv("output/resources_by_dataset.csv")

atipp_requests <- packages |> 
  filter(type == "access-requests") |> 
  mutate(
    fiscal_year = substr(name, 0L, 2L)
  )

atipp_requests |> 
  count(fiscal_year) |> 
  write_csv("output/atipp_requests_by_fiscal_year.csv")
