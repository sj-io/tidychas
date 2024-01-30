#' Get HUD CHAS data for a geography.
#'
#' @param geography The geography of your data:
#'   * `"state"`
#'   * `"county"`
#'   * `"tract"`: census tracts
#'   * `"place"`: census places (cannot narrow by county)
#'   * `"counties by place"`: counties divided into places
#'   * `"mcd"`: minor civil divisions (such as county districts)
#'   * `"consolidated cities"`: consolidated city-county gov't
#' @param year Numeric end year of the data (defaults to 2020)
#' @param state Character string of two-digit state FIPS code (ex: "47")
#' @param county Character string of three-digit county FIPS code (ex: "157")
#' @param keep_zip Keep the downloaded zip file for future use?
#'   * `TRUE`: Keep the zip file(the default)
#'   * `FALSE`: Delete the zip file after use.
#'
#' @import purrr tidyr stringr arrow dplyr httr rappdirs
#'
#' @return A tibble of the requested data.
#'
#' @export
#' @examples
#' get_chas("state", state = "47") |> head()
get_chas <- function(geography, year = NULL, state = NULL, county = NULL, keep_zip = TRUE) {

  # year for chas data defaults to 2020
  if (is.null(year)) {
    year <- 2020
  }
  year0 <- (year - 4)

  # turn geography into a fips code
  fips_geo <- chas_fips_geos$fips[chas_fips_geos$geo == geography]
  fips_geo_num <- as.numeric(fips_geo)

  # define tidychas cache & arrow cache
  cache_dir <- user_cache_dir("tidychas")

  if (!file.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE)
    message("Created tidychas cache.")
  }

  data_dir <- file.path(cache_dir, "data")
  data_set <- file.path(data_dir, paste0("year=", year), paste0("geography=", fips_geo_num))
  variables_dir <- file.path(cache_dir, "variables")
  variables_file <- file.path(variables_dir, paste0(year, ".csv"))

  if (!file.exists(data_dir)) {
    dir.create(data_dir, recursive = TRUE)
    dir.create(variables_dir, recursive = TRUE)
  }

  # check if data already exists in cache
  existing_files <- list.files(data_set, recursive = TRUE, pattern = ".csv")
  the_schema <- schema(year = int32(), geography = int32(), st = int64(), cnty = int64(), geoid = int64(), name = string(), variable = string(), est = int64(), moe = int64())

  if (length(existing_files) == 0) {
    no_data <- TRUE
  } else {
    cache_data <- open_dataset(data_dir, format = "csv", convert_options = csv_convert_options(check_utf8 = FALSE), partitioning = c("year", "geography")) |>
      filter(year == {{ year }} & geography == {{ fips_geo_num }})
    existing_data <- cache_data

    if (!is.null(state) & geography != "remainders") {
      existing_data <- existing_data |> filter(st == as.numeric({{state}}))
    }

    if (!is.null(county) & geography %in% c("tract", "counties by place", "county", "mcd")) {
      existing_data <- existing_data |> filter(cnty == as.numeric({{county}}))
    }

    existing_data <- existing_data |> collect()

    if (nrow(existing_data) > 0) {
      no_data <- FALSE
      message("Retrieving data from cache.")
      chas_dictionary <- read_csv_arrow(variables_file)
      chas_data <- existing_data
    } else {
      no_data <- TRUE
    }
  }

  if (no_data == TRUE) {
    # get data
    url <- sprintf("https://www.huduser.gov/portal/datasets/cp/%sthru%s-%s-csv.zip", as.character(year0), as.character(year), fips_geo)
    zip_file <- file.path(cache_dir, basename(url))
    unzip_dir <- file.path(cache_dir, gsub(".zip", "", basename(url)))

    if (!file.exists(zip_file)) {
      message("Retrieving zip file from HUD website. Small geographies may take a long time to download.")
      httr::GET(url, write_disk(zip_file, overwrite = TRUE))
    }

    if (!file.exists(unzip_dir)) {
      dir.create(unzip_dir, recursive = TRUE)
    }

    unzip(zip_file, exdir = unzip_dir, overwrite = TRUE)

    if (!file.exists(variables_file)) {
      chas_dictionary_file <- list.files(unzip_dir, pattern = ".xlsx", recursive = TRUE, full.names = TRUE)
      chas_dictionary <- chas_make_dictionary(chas_dictionary_file)
      write_csv_arrow(chas_dictionary, variables_file)
      message("Created data dictionary for the given year and caching for future use.")
    } else {
      chas_dictionary <- read_csv_arrow(variables_file)
    }

    chas_csv_files <- list.files(unzip_dir, full.names = TRUE, recursive = TRUE, pattern = ".csv")

    message("Filtering data for specified geography.")
    walk(chas_csv_files, ~ chas_narrow_csv(., geography = {{geography}}, state = {{state}}, county = {{county}}), .progress = TRUE)

    # tidy each parquet file
    message("Converting data to tidy format.")
    chas_data <- map(chas_csv_files, ~ chas_tidy(., chas_dictionary, geography = {{geography}}), .progress = TRUE) |> list_rbind()

    # save data in cache
    if (length(existing_files) != 0) {
      save_table <- concat_tables(as_arrow_table(cache_data, schema = the_schema), as_arrow_table(chas_data, schema = the_schema))
      write_dataset(save_table, data_dir, partitioning = c("year", "geography"), format = "csv", max_rows_per_file = 1e6)
      message("Data cached for quicker use in future.")
    } else {
      write_dataset(chas_data, data_dir, partitioning = c("year", "geography"), format = "csv", max_rows_per_file = 1e6)
      message("Data cached for quicker use in future.")
    }

    message("Cleaning up cache.")
    unlink(unzip_dir, recursive = TRUE, force = TRUE)

    if (keep_zip == FALSE) {
      file.remove(zip_file)
    }

  }

  chas_data |>
    left_join(chas_dictionary, by = "variable") |>
    relocate(c(est, moe), .after = last_col()) |>
    relocate(c(year, geography), .before = 1)

}

#' Create a data dictionary of all variables
#' @param chas_dictionary_file An excel file in each CHAS unzip folder with a sheet defining geographies all variables in all tables.
#' @import readxl
#' @return a dataset containing a list of all variables in all tables.
chas_make_dictionary <- function(chas_dictionary_file) {
  chas_raw_dictionary <- readxl::read_excel(chas_dictionary_file, sheet = "All Tables", col_types = "text") |> janitor::clean_names()
  chas_raw_dictionary <- chas_raw_dictionary |>
    select(variable = ends_with("_name"), starts_with("description_")) |>
    filter(str_starts(variable, "T.*_est"))

  chas_vars_label <- chas_variables$label
  names(chas_vars_label) <- chas_variables$original

  chas_vars_concept <- chas_variables$concept
  names(chas_vars_concept) <- chas_variables$original

  empty_chas <- chas_raw_dictionary |>
    rename(universe = description_1) |>
    filter(if_all(c(description_2:description_5), ~ . %in% c("All", NA_character_))) |>
    mutate(label = "All",
           universe = str_replace_all(universe, chas_universe),
           concept = "Total") |>
    select(variable, universe, label, concept)

  chas_raw_dictionary |>
    anti_join(empty_chas, by = "variable") |>
    rename(universe = description_1) |>
    pivot_longer(cols = starts_with("description_"), names_to = "d_num") |>
    filter(!(value %in% c("All", NA_character_))) |>
    mutate(
      universe = str_replace_all(universe, chas_universe),
      label = str_replace_all(value, chas_vars_label) |> str_to_sentence() |>
        str_replace_all(c("hamfi" = "HAMFI",
                          "vhud" = "VHUD",
                          "rhud" = "RHUD"
        )),
      concept = str_replace_all(value, chas_vars_concept) |> str_to_title()
    ) |>
    select(-value) |>
    pivot_wider(id_cols = c(variable, universe), names_from = d_num, values_from = c(label, concept)) |>
    unite(col = label, starts_with("label_"), sep = "!!", na.rm = TRUE) |>
    unite(col = concept, starts_with("concept_"), sep = " by ", na.rm = TRUE) |>
    bind_rows(empty_chas) |>
    mutate(variable = str_remove(variable, "est"))

}

# convert csv files to parquet
chas_narrow_csv <- function(chas_csv_file, geography, state = NULL, county = NULL) {

  df <- read_csv_arrow(chas_csv_file, convert_options = csv_convert_options(check_utf8 = FALSE), as_data_frame = FALSE)

  if (!is.null(state)) {
    df <- df |> filter(st == as.numeric(state))
  }

  if (!is.null(county) & !(geography %in% c("place", "consolidated cities"))) {
    df <- df |> filter(cnty == as.numeric(county))
  }

  df <- df |>
    rename(year = source, geography = sumlevel) |>
    mutate(
      year = gsub(".*thru", "", year),
      geoid = gsub(".*US", "", geoid)
    ) |>
    write_csv_arrow(chas_csv_file)
}

# combine tables into one
chas_tidy <- function(chas_file, chas_dictionary, geography) {
    df <- read_csv_arrow(chas_file, convert_options = csv_convert_options(check_utf8 = FALSE))

  df <- df |>
    pivot_longer(
      cols = starts_with("T", ignore.case = FALSE),
      names_pattern = "(T.*)_(.{3})(\\d+)",
      names_to = c("table", ".value", "var")
    ) |>
    unite(variable, c(table, var), sep = "_", na.rm = TRUE)

  if (geography %in% c("state", "remainder", "place")) {
    df <- df |> mutate(cnty = NA_integer_)
  }
  invisible(col_moe <- df$moe)
  if (is.null(col_moe)) {
    df <- df |> mutate(moe = NA_integer_)
  }

  df |>
    select(year, geography, st, cnty, geoid, name, variable, est, moe)
}
