library(tidyverse)

packages <- read_csv("output/packages_by_visits_and_downloads.csv")

atipp_requests <- packages |> 
  filter(type == "access-requests") |> 
  mutate(
    fiscal_year = substr(name, 0L, 2L)
  )

atipp_requests |> 
  count(fiscal_year) |> 
  write_csv("output/atipp_requests_by_fiscal_year.csv")


# Draft scoring criteria
# 2026-06-10 for discussion
organization_name_mapping = tribble(
  ~organization_name, ~organization_label,
  "atipp-office", "ATIPP Office",
  "community-services", "Community Services",
  "economic-development-tourism-and-culture", "Economic Development, Tourism and Culture",
  "economic-development-tourism-and-culture", "Tourism and Culture",
  "economic-development-tourism-and-culture", "Economic Development",
  "education", "Education",
  "elections-yukon", "Elections Yukon",
  "energy-mines-and-resources", "Energy, Mines and Resources",
  "environment", "Environment",
  "executive-council-office", "Executive Council Office",
  "finance", "Finance",
  "french-language-services-directorate", "French Language Services Directorate",
  "geomatics-yukon", "Geomatics Yukon",
  "health-and-social-services", "Health and Social Services",
  "highways-and-public-works", "Highways and Public Works",
  "justice", "Justice",
  "procurement-support-centre", "Procurement Support Centre",
  "public-service-commission", "Public Service Commission",
  "women-and-gender-equity-directorate", "Women and Gender Equity Directorate",
  "yukon-bureau-of-statistics", "Yukon Bureau of Statistics",
  "yukon-development-corporation", "Yukon Development Corporation",
  "yukon-energy-corporation", "Yukon Energy Corporation",
  "yukon-geological-survey", "Yukon Geological Survey",
  "yukon-hospital-corporation", "Yukon Hospital Corporation",
  "yukon-housing-corporation", "Yukon Housing Corporation",
  "yukon-liquor-corporation", "Yukon Liquor Corporation",
  "yukon-lottery-corporation", "Yukon Lottery Corporation",
  "yukon-police-council", "Yukon Police Council",
  "yukon-university", "Yukon University",
  "yukon-workers-compensation-health-and-safety-board", "Workers’ Compensation Health and Safety Board",
)

# TODO: map ATIPP office, PSc, Geomatics, YGS, and YBS to their parent departments

resources <- read_csv("output/resources_by_scoring_evaluation_criteria.csv")

# Resource name might be e.g. "Highways and Public Works organizational chart.pdf"
detect_department_from_resource_name <- function(resource_name) {
  
  organization_name_mapping_output <- organization_name_mapping |> 
    mutate(
      name_matches = case_when(
        str_detect(resource_name, organization_label) ~ TRUE,
        .default = FALSE
      )
    )
  
  organization_name_mapping_output |> 
    filter(name_matches == TRUE) |> 
    select(organization_name) |> 
    first() |> 
    pull(organization_name)

}

# Add a matching department to ATIPP Office publications
resources <- resources |> 
  mutate(
    derive_organization_name = case_when(
      organization_name == "atipp-office" & !is.na(publication_type_under_atipp_act) ~ TRUE,
      .default = FALSE
    ) ,
    # Not the most efficient way to do this, compare across all resource names and then filter down :P
  ) |> 
  mutate(
    derived_organization_name_from_resource_name = map_chr(
      resources_name, detect_department_from_resource_name
    )
  ) |> 
  mutate(
    derived_organization_name = case_when(
      derive_organization_name == TRUE ~ derived_organization_name_from_resource_name,
      .default = organization_name
    )
  )


