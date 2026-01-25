library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(readr)
library(tidyverse)
library(tibble)
library(lubridate)


#import last run so can keep data for locations that aren't in latest update
lastrun <- read_csv("snowfall_totals.csv")

#REMOVE FROM LASTRUN RECORDS THAT ARE MORE THAN X HOURS OLD
lastrun <- lastrun %>%
  # Step 1: Combine date + 24-hour time into proper datetime
  mutate(
    datetime_et = as.POSIXct(
      paste(date, time_fixed),          # e.g. "1/22/2026 03:00:00"
      format = "%m/%d/%Y %H:%M:%S",    # 24-hour hour:minute:second
      tz = "America/New_York"           # explicit Eastern Time
    ),
    # Step 2: Convert to UTC for filtering
    datetime_utc = with_tz(datetime_et, tzone = "UTC")
  )

# Step 3: Current time in UTC
current_utc <- with_tz(Sys.time(), tzone = "UTC")

# Step 4: Filter last X hours **** CHANGE NUMBER OF HOURS HERE ****
recent_lastrun <- lastrun %>%
  filter(difftime(current_utc, datetime_utc, units = "hours") <= 4)

#match columns and time format for later merge
recent_lastrun <- recent_lastrun %>%
  # Convert time_fixed back to 12-hour AM/PM using datetime
  mutate(
    time_fixed = format(datetime, "%-I:%M %p")  # %-I removes leading 0 from hour
  ) %>%
  # Drop intermediate columns if they still exist
  select(-datetime_et, -datetime_utc) %>%
  # Make datetime the last column
  relocate(datetime, .after = last_col())

# FUNCTION to fetch snowfall data from a given office
fetch_snowfall_data <- function(office_code, user_agent) {
  base_url <- "https://api.weather.gov"
  
  # 1. Get list of PNS products
  pns_list <- GET(
    paste0(base_url, "/products/types/PNS/locations/", office_code),
    user_agent
  ) |> content(as = "parsed", type = "application/json")
  
  # Get products dataframe
  products_df <- bind_rows(pns_list$`@graph`) |>
    arrange(desc(issuanceTime))
  
  # 2. Find the most recent Spotter Reports with snow data
  spotter_product <- NULL
  content_text <- NULL
  
  # Define search patterns based on office
  if (office_code == "LWX") {
    header_pattern <- "Spotter Reports"
  } else if (office_code == "PHI") {
    # PHI uses various patterns - check for any snowfall-related header
    header_pattern <- "SNOWFALL REPORTS|SNOW.*REPORTS|FREEZING RAIN REPORTS"
  } else if (office_code == "AKQ") {
    header_pattern <- "Spotter Reports|SNOWFALL REPORTS|SNOW.*REPORTS"
  } else {
    header_pattern <- "Spotter Reports"  # default
  }
  
  for (i in 1:min(20, nrow(products_df))) {  # Check up to 20 recent products
    product_id <- products_df$`@id`[i]
    
    # Fetch the product
    product <- GET(
      product_id,
      user_agent
    ) |> content(as = "parsed", type = "application/json")
    
    text <- product$productText
    
    # Check if it's a snowfall reports product with metadata
    if (str_detect(text, regex(header_pattern, ignore_case = TRUE)) && 
        str_detect(text, fixed("**METADATA**")) &&
        str_detect(text, "SNOW")) {
      
      spotter_product <- product
      content_text <- text
      cat("Found", office_code, "Snowfall Reports from:", products_df$issuanceTime[i], "\n")
      break
    }
  }
  
  # Check if we found a product
  if (is.null(spotter_product)) {
    warning(paste("No recent snowfall reports with data found for", office_code))
    return(NULL)
  }
  
  # 3. Extract METADATA section
  metadata_parts <- str_split(content_text, fixed("**METADATA**"))[[1]]
  
  if (length(metadata_parts) < 2) {
    warning(paste("No METADATA section found for", office_code))
    return(NULL)
  }
  
  metadata_text <- metadata_parts[2]
  
  # 4. Parse metadata lines
  metadata_lines <- metadata_text |>
    str_split("\n") |>
    unlist() |>
    str_trim() |>
    keep(~ str_starts(.x, ":"))
  
  if (length(metadata_lines) == 0) {
    warning(paste("No metadata entries found for", office_code))
    return(NULL)
  }
  
  metadata_df <- metadata_lines |>
    str_remove("^:") |>
    paste(collapse = "\n") |>
    read_csv(
      col_names = c(
        "date", "time", "state", "county", "location",
        "distance", "direction", "lat", "lon",
        "phenomenon", "value", "unit",
        "source", "description"
      ),
      show_col_types = FALSE
    )
  
  # 5. Filter storm-total snowfall for MD only - now includes SNOW_24 and 24 hour snowfall
  snowfall_data <- metadata_df |>
    filter(
      phenomenon %in% c("SNOW", "SNOW_24"),
      description %in% c("Storm Total Snow", "Storm total snowfall", "24 hour snowfall"),
      state == "MD"  # Only Maryland records
    ) |>
    mutate(
      value = as.numeric(value),
      lat = as.numeric(lat),
      lon = as.numeric(lon),
      office = office_code  # Add office identifier
    )
  
  return(snowfall_data)
}

# Set up user agent
ua <- user_agent("lwx-snowfall-r/1.0 (searley@baltsun.com)")

# Fetch data from all three offices
lwx_data <- fetch_snowfall_data("LWX", ua)
phi_data <- fetch_snowfall_data("PHI", ua)
akq_data <- fetch_snowfall_data("AKQ", ua)

# Combine data from all offices
# Start with an empty list to collect non-null data
data_list <- list()

if (!is.null(lwx_data) && nrow(lwx_data) > 0) {
  data_list <- append(data_list, list(lwx_data))
}

if (!is.null(phi_data) && nrow(phi_data) > 0) {
  data_list <- append(data_list, list(phi_data))
}

if (!is.null(akq_data) && nrow(akq_data) > 0) {
  data_list <- append(data_list, list(akq_data))
}

# Check if we have any data
if (length(data_list) == 0) {
  stop("No MD snowfall data found from LWX, PHI, or AKQ offices")
}

# Combine all available data
snowfall_totals <- bind_rows(data_list)

snowfall_totals <- snowfall_totals |>
  arrange(desc(value))

# Check if we got any data
if (nrow(snowfall_totals) == 0) {
  stop("No snowfall data found from LWX, PHI, or AKQ offices")
}

cat("\nTotal MD snowfall records found:", nrow(snowfall_totals), "\n")
cat("From LWX:", ifelse(is.null(lwx_data), 0, nrow(lwx_data)), "\n")
cat("From PHI:", ifelse(is.null(phi_data), 0, nrow(phi_data)), "\n")
cat("From AKQ:", ifelse(is.null(akq_data), 0, nrow(akq_data)), "\n\n")

# Add POSIXct datetime column
snowfall_totals$time_fixed <- str_replace(
  snowfall_totals$time,
  "^([0-9]{1,2})([0-9]{2})",
  "\\1:\\2"
)

# Parse datetime, handling both formats with and without leading space
snowfall_totals$datetime <- mdy_hm(
  paste(snowfall_totals$date, str_trim(snowfall_totals$time_fixed)),
  tz = "America/New_York"
)

# Debug: check for any NA datetimes
if (any(is.na(snowfall_totals$datetime))) {
  cat("Warning: Some datetimes failed to parse\n")
  cat("Sample problematic entries:\n")
  print(head(snowfall_totals[is.na(snowfall_totals$datetime), c("date", "time", "time_fixed")]))
}

#COMMENTING OUT -- TIMES SEEM TO BE CONVERTING WRONG
#rescue recent_lastrun records whose locations aren't in snowfall_totals
#rows_to_add <- recent_lastrun %>%
# anti_join(
#snowfall_totals,
#by = c("location", "county")
# )

#snowfall_totals <- bind_rows(
#snowfall_totals,
#rows_to_add
#)



# Export complete data as CSV (before time filter)
write_csv(snowfall_totals, "snowfall_totals.csv")

# Filter out records before 11 AM EST on Jan 25, 2026
cutoff_time <- as.POSIXct("2026-01-25 11:00:00", tz = "America/New_York")

cat("\nFiltering records before 11 AM EST Jan 25, 2026\n")
cat("Cutoff time:", format(cutoff_time, "%Y-%m-%d %I:%M %p %Z"), "\n")
cat("Records before filter:", nrow(snowfall_totals), "\n")

snowfall_totals <- snowfall_totals %>%
  filter(datetime >= cutoff_time)

cat("Records after 11 AM EST Jan 25 filter:", nrow(snowfall_totals), "\n\n")

#construct map data (using filtered data)
date_parsed <- as.Date(snowfall_totals$date, format = "%m/%d/%Y")

time_display <- paste0(
  snowfall_totals$time_fixed,
  " ",
  as.integer(format(date_parsed, "%m")),
  "/",
  as.integer(format(date_parsed, "%d"))
)


datetime_utc <- as.POSIXct(
  snowfall_totals$datetime,
  tz = "America/New_York"
)
datetime_utc <- as.POSIXct(format(datetime_utc, tz = "UTC"), tz = "UTC")


latency_minutes <- as.numeric(
  difftime(
    Sys.time(),
    datetime_utc,
    units = "mins"
  )
)

mapdata <- data.frame(
  Location  = snowfall_totals$location,
  Measurement = snowfall_totals$value,
  Time = time_display,
  Lattitude = snowfall_totals$lat,
  Longitude = snowfall_totals$lon,
  Latency = -round(latency_minutes)
)


write_csv(mapdata,"mapdata.csv")
