source("load.R")

# output |> View()

# Add year created and year modified
output <- output |> 
  mutate(
    year_created = str_sub(metadata_created, 0L, 4L),
    year_modified = str_sub(metadata_modified, 0L, 4L),
  )

# Skip "dataset" packages since these aren't categorized properly
output <- output |> 
  filter(type != "dataset")

packages_by_type <- output |> 
  group_by(
    type
  ) |> 
  summarise(
    packages = n(),
    resources = sum(num_resources)
  )

packages_by_type_by_org <- output |> 
  group_by(
    type,
    organization_name
  ) |> 
  summarise(
    packages = n(),
    resources = sum(num_resources)
  )

packages_by_type_by_year_by_org <- output |> 
  group_by(
    type,
    organization_name, 
    year_created
  ) |> 
  summarise(
    packages = n(),
    resources = sum(num_resources)
  )

packages_by_type_by_year <- output |> 
  group_by(
    type,
    year_created
  ) |> 
  summarise(
    packages = n(),
    resources = sum(num_resources)
  )



# Write files out to CSV --------------------------------------------------

# TODO: write this as a mappable purrr function

packages_by_type |> 
  write_out_csv("packages_by_type")

packages_by_type_by_org |> 
  write_out_csv("packages_by_type_by_org")

packages_by_type_by_year_by_org |> 
  write_out_csv("packages_by_type_by_year_by_org")

packages_by_type_by_year |> 
  write_out_csv("packages_by_type_by_year")

