library(tidyverse)
library(lubridate)

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

# Switch derived_organization_name to use parent department for sub-departmental level CKAN organizations
resources <- resources |> 
  mutate(
    derived_organization_name = case_when(
      derived_organization_name == "atipp-office" ~ "highways-and-public-works",
      derived_organization_name == "geomatics-yukon" ~ "highways-and-public-works",
      derived_organization_name == "procurement-support-centre" ~ "highways-and-public-works",
      derived_organization_name == "yukon-bureau-of-statistics" ~ "finance",
      derived_organization_name == "yukon-geological-survey" ~ "energy-mines-and-resources",
      .default = derived_organization_name
    )
  )

# Cleanup the extra calculation columns
resources <- resources |> 
  select(
    ! any_of(
      c(
        "derive_organization_name",
        "derived_organization_name_from_resource_name"
      )
    )
  )


# Organizations to score --------------------------------------------------

organizations_to_score <- organization_name_mapping |> 
  filter(
    ! organization_name %in% c(
      "atipp-office",
      "geomatics-yukon",
      "procurement-support-centre",
      "yukon-bureau-of-statistics",
      "yukon-geological-survey",
      "yukon-university",
      "yukon-police-council"
    )
  )

# Scoring sections --------------------------------------------------------

# Time ranges for resources (edited within a recency period)
resources <- resources |> 
  mutate(
    resources_latest_date_modified = case_when(
      is.na(resources_last_modified) ~ resources_metadata_modified,
      # resources_last_modified > resources_metadata_modified ~  resources_last_modified,
      .default = resources_last_modified
    ),
    updated_within_36_months = case_when(
      now() - months(36) < resources_latest_date_modified ~ TRUE,
      .default = FALSE
    ),
    updated_within_24_months = case_when(
      now() - months(24) < resources_latest_date_modified ~ TRUE,
      .default = FALSE
    ),
    updated_within_18_months = case_when(
      now() - months(18) < resources_latest_date_modified ~ TRUE,
      .default = FALSE
    ),
    updated_within_12_months = case_when(
      now() - months(12) < resources_latest_date_modified ~ TRUE,
      .default = FALSE
    )
  )

# Scoring weights

score_did_not_meet <- 0
score_met <- 1
score_exceeded <- 2

# A01. the public body’s organizational structure

calculate_score_a01 <- function(organization_name) {
  
  count_within_36_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_type_under_atipp_act == "organizational_structures",
      updated_within_36_months == TRUE
    ) |> 
    count()
  
  count_within_18_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_type_under_atipp_act == "organizational_structures",
      updated_within_18_months == TRUE
    ) |> 
    count()
  
  if(count_within_18_months > 0) {
    return(score_exceeded)
  }
  else if(count_within_36_months > 0) {
    return(score_met)
  }
  
  return(score_did_not_meet)
  
}

organizations_to_score <- organizations_to_score |> 
  mutate(
    score_a01 = map_int(organization_name, calculate_score_a01)
  )

# A02. responsibilities and functions

calculate_score_a02 <- function(organization_name) {
  
  count_overall <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_type_under_atipp_act == "organizational_responsibilities_and_functions"
    ) |> 
    count()
  
  count_within_24_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_type_under_atipp_act == "organizational_responsibilities_and_functions",
      updated_within_24_months == TRUE
    ) |> 
    count()
  
  if(count_within_24_months > 0) {
    return(score_exceeded)
  }
  else if(count_overall > 0) {
    return(score_met)
  }
  
  return(score_did_not_meet)
  
}

organizations_to_score <- organizations_to_score |> 
  mutate(
    score_a02 = map_int(organization_name, calculate_score_a02)
  )


# A03. current manuals and policy statements

calculate_score_a03 <- function(organization_name) {
  
  count_within_36_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_type_under_atipp_act == "departmental_manuals_and_policy_statements",
      updated_within_36_months == TRUE
    ) |> 
    count()
  
  count_within_24_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_type_under_atipp_act == "departmental_manuals_and_policy_statements",
      updated_within_24_months == TRUE
    ) |> 
    count()
  
  if(count_within_24_months > 0) {
    return(score_exceeded)
  }
  else if(count_within_36_months > 0) {
    return(score_met)
  }
  
  return(score_did_not_meet)
  
}

organizations_to_score <- organizations_to_score |> 
  mutate(
    score_a03 = map_int(organization_name, calculate_score_a03)
  )

# A04. public opinion polls, research studies, etc.

calculate_score_a04 <- function(organization_name) {
  
  count_within_36_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      ! publication_type_under_atipp_act %in% c(
        "organizational_structures",
        "organizational_responsibilities_and_functions",
        "departmental_manuals_and_policy_statements",
        "information_or_record_available_to_public_without_access_request"
      ),
      !is.na(publication_type_under_atipp_act),
      publication_type_under_atipp_act != "na",
      updated_within_36_months == TRUE
    ) |> 
    count()
  
  count_within_12_months <- resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      ! publication_type_under_atipp_act %in% c(
        "organizational_structures",
        "organizational_responsibilities_and_functions",
        "departmental_manuals_and_policy_statements",
        "information_or_record_available_to_public_without_access_request"
      ),
      !is.na(publication_type_under_atipp_act),
      publication_type_under_atipp_act != "na",
      updated_within_36_months == TRUE
    ) |> 
    count()
  
  if(count_within_12_months >= 2 | count_within_36_months >= 5) {
    return(score_exceeded)
  }
  else if(count_within_36_months >= 2) {
    return(score_met)
  }
  
  return(score_did_not_meet)
  
}

organizations_to_score <- organizations_to_score |> 
  mutate(
    score_a04 = map_int(organization_name, calculate_score_a04)
  )

# A05. information in the public interest

calculate_score_a05 <- function(organization_name) {
  
  eligible_resources <- resources |> 
    mutate(
      publication_available_to_the_public = case_when(
        publication_type_under_atipp_act == "information_or_record_available_to_public_without_access_request" ~ TRUE,
        type == "information" & is.na(publication_type_under_atipp_act) ~ TRUE,
        .default = FALSE
      )
  )
  
  count_within_36_months <- eligible_resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_available_to_the_public == TRUE,
      updated_within_36_months == TRUE
    ) |> 
    count()
  
  count_within_12_months <- eligible_resources |> 
    filter(
      derived_organization_name == {{organization_name}},
      publication_available_to_the_public == TRUE,
      updated_within_12_months == TRUE
    ) |> 
    count()
  
  if(count_within_12_months >= 2 | count_within_36_months >= 5) {
    return(score_exceeded)
  }
  else if(count_within_36_months >= 2) {
    return(score_met)
  }
  
  return(score_did_not_meet)
  
}

organizations_to_score <- organizations_to_score |> 
  mutate(
    score_a05 = map_int(organization_name, calculate_score_a05)
  )

organizations_to_score |> 
  write_csv("output/organization_scoring_evaulation.csv")
