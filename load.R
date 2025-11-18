library(tidyverse)
library(fs)
library(readxl)
library(rmarkdown)
library(janitor)
library(ckanr)

run_start_time <- now()
paste("Start time:", run_start_time)

if(file_exists(".env")) {
  readRenviron(".env")
  
  ckan_url <- Sys.getenv("ckan_url")
  ckan_api_token <- Sys.getenv("ckan_api_token")
  
}

offset_increment = 100
max_runs = 100

ckanr_setup(
  url = ckan_url, 
  key = ckan_api_token
  )

# dashboard_activity_list()

# results <- package_list()

# results <- package_list_current(as = 'table')

get_package_resource_totals <- function(offset = 0, offset_increment = 10) {
  
  # Fields to keep
  # id, title, num_resources, type, organization
  
  results <- package_list_current(
    as = 'table',
    offset = offset,
    limit = offset_increment
  )
  
  if(length(results) == 0) {
    # We're past the end of the results
    return(results)
  }
  
  results <- results |> 
    select(id, name, title, num_resources, type, organization, metadata_created, metadata_modified) |> 
    unnest(
      organization,
      names_sep = "_"
    ) |> 
    select(id, name, title, num_resources, type, organization_name, metadata_created, metadata_modified)
  
  results  
  
}

current_offset <- 0
run_count <- 0

loop_get_package_resource_totals <- function() {
  
  while(run_count <= max_runs) {
    
    cat("Current run", run_count, " starting at ", current_offset, "\n")
    
    if(current_offset == 0) {
      # First run
      output <- get_package_resource_totals(current_offset, offset_increment)
    }
    else {
      new_output <- get_package_resource_totals(current_offset, offset_increment)
      
      # If it's an empty list, that means we're past the end of the existing resources
      if(length(new_output) == 0) {
        cat("Ending run at ", run_count, "\n")
        break
      }
      
      output <- output |> 
        bind_rows(new_output)
    }
    
    current_offset <- current_offset + offset_increment
    run_count <- run_count + 1
    
    Sys.sleep(0.5)
    
  }

  output
  
}


# Max amount


# output <- get_package_resource_totals()
# 
# View(output)
# 
# example_output <- package_list_current(as = "table", offset = 3600, limit = 100)
# length(example_output)

output <- loop_get_package_resource_totals()

# View(output)


output |> 
  write_csv(
    file = "output/resources_by_dataset.csv"
  )

run_end_time <- now()
paste("Start time was:", run_start_time)
paste("End time was:", run_end_time)

paste("Elapsed time was", round(time_length(interval(run_start_time, run_end_time), "hours"), digits = 2), "hours")
