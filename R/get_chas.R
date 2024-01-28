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
#'
#' @import purrr tidyr stringr arrow dplyr httr rappdirs
#'
#' @return A tibble of the requested data.
#'
#' @export
#' @examples
#' get_chas("state", state = "47") |> head()
get_chas <- function(geography, year = NULL, state = NULL, county = NULL) {

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

  arrow_dir <- file.path(cache_dir, "arrow")

  if (!file.exists(arrow_dir)) {
    dir.create(arrow_dir, recursive = TRUE)
  }

  # check if data already exists in cache
  existing_data <- list.files(arrow_dir, recursive = TRUE)

  if (length(existing_data) == 0) {
    no_data <- TRUE
  } else {
    existing_data <- open_dataset(arrow_dir, partitioning = c("year", "geography", "st", "cnty")) |>
      filter(year == {{ year }} & geography == {{ fips_geo_num }})

    if (!is.null(state) & geography != "remainders") {
      existing_data <- existing_data |> filter(st == as.numeric(state))
    }

    if (!is.null(county) & geography %in% c("tract", "counties by place", "county", "mcd")) {
      existing_data <- existing_data |> filter(cnty == as.numeric(county))
    }

    existing_data <- existing_data |> collect()

    if (nrow(existing_data) > 0) {
      no_data <- FALSE
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
      unzip(zip_file, exdir = unzip_dir, overwrite = TRUE)
    }

    chas_csv_files <- list.files(file.path(unzip_dir), full.names = TRUE, recursive = TRUE, pattern = ".csv")

    if (length(chas_csv_files) == 0) {
      unzip(zip_file, exdir = unzip_dir, overwrite = TRUE)
    }

    # data dictionary
    chas_dictionary_file <- list.files(unzip_dir, pattern = ".xlsx", recursive = TRUE, full.names = TRUE)
    chas_dictionary <- chas_make_dictionary(chas_dictionary_file)

    # function to convert tables into one dataset
    chas_read_file <- function(chas_file) {
      if (str_ends(chas_file, "csv")) {
      # csv files not converted to parquet
        df <- read_csv_arrow(chas_file, convert_options = csv_convert_options(check_utf8 = FALSE)) |>
          rename(year = source, geography = sumlevel)

        if (geography != "remainders") {
          df <- df |> rename(place = name)
        }

        df <- df |>
          mutate(
            year = gsub(".*thru", "", year),
            geoid = gsub(".*US", "", geoid),
            geography = geography
          )

      } else {
        # parquet files
        df <- read_parquet(chas_file, as_data_frame = FALSE)
        df <- df |> filter(st == as.numeric(state))

        if (!is.null(county) & !(geography %in% c("place", "consolidated cities"))) {
          df <- df |> filter(cnty == as.numeric(county))
        }

        df <- df |> collect()
      }

      # tidy dataset by pivoting variable columns into long format
      # join with data dictionary
      df |>
        pivot_longer(
          cols = starts_with("T", ignore.case = FALSE),
          names_pattern = "(T.*)_(.{3})(\\d+)",
          names_to = c("table", ".value", "variable")
        ) |>
        unite(name, c(table, variable), sep = "_", na.rm = TRUE) |>
        left_join(chas_dictionary, by = "name")
        # relocate(c(est, moe), .after = last_col())
    }

    if (geography %in% c("state", "remainders")) {
      chas_data <- map(chas_csv_files, chas_read_file, .progress = TRUE) |> list_rbind()
      write_dataset(chas_data, arrow_dir, partitioning = c("year", "geography"))

      if (geography == "state" & !is.null(state))
        chas_data <- chas_data |> filter(st == as.numeric(state))

    } else {

      chas_parquet_files <- list.files(file.path(unzip_dir), full.names = TRUE, recursive = TRUE, pattern = ".parquet")
      if (length(chas_parquet_files) == 0) {

        # function to convert csv files to parquet
        chas_csv_to_parquet <- function(chas_csv_file) {
          chas_parquet_file <- gsub("csv$", "parquet", chas_csv_file)

          df <- open_csv_dataset(chas_csv_file, convert_options = csv_convert_options(check_utf8 = FALSE))

          if (geography == "place") {
            df <- df |> rename(place_code = place)
          }

          df <- df |>
            rename(year = source, geography = sumlevel, place = name) |>
            mutate(
              year = gsub(".*thru", "", year),
              geoid = gsub(".*US", "", geoid),
              geography = geography
            ) |>
            write_parquet(chas_parquet_file)

        }

        # map the function to each csv file
        purrr::walk(chas_csv_files, chas_csv_to_parquet)
        chas_parquet_files <- list.files(file.path(unzip_dir), full.names = TRUE, recursive = TRUE, pattern = ".parquet")
      }

      chas_data <- purrr::map(chas_parquet_files, chas_read_file, .progress = TRUE) |> list_rbind()

      # save data in cache
      if (geography %in% c("tract", "counties by place", "mcd")) {
        write_dataset(chas_data, arrow_dir, partitioning = c("year", "geography", "st", "cnty"))
      } else {
        write_dataset(chas_data, arrow_dir, partitioning = c("year", "geography", "st"))
      }

    }
  }

  chas_data

}

#' Create a data dictionary of all variables
#' @param chas_dictionary_file An excel file in each CHAS unzip folder with a sheet defining geographies all variables in all tables.
#' @import readxl
#' @return a dataset containing a list of all variables in all tables.
chas_make_dictionary <- function(chas_dictionary_file) {
  chas_raw_dictionary <- readxl::read_excel(chas_dictionary_file, sheet = "All Tables", col_types = "text") |> janitor::clean_names()
  chas_raw_dictionary <- chas_raw_dictionary |>
    select(name = ends_with("_name"), starts_with("description_")) |>
    filter(str_starts(name, "T.*_est"))

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
    select(name, universe, label, concept)

  chas_raw_dictionary |>
    anti_join(empty_chas, by = "name") |>
    rename(universe = description_1) |>
    pivot_longer(cols = starts_with("description_"), names_to = "d_num") |>
    filter(!(value %in% c("All", NA_character_))) |>
    mutate(
      universe = str_replace_all(universe, chas_universe),
      label = str_replace_all(value, chas_vars_label) |> str_to_sentence() |>
        str_replace_all(c("\\bhamfi" = "HAMFI",
                          "\\bvhud" = "VHUD",
                          "\\brhud" = "RHUD"
        )),
      concept = str_replace_all(value, chas_vars_concept) |> str_to_title()
    ) |>
    select(-value) |>
    pivot_wider(id_cols = c(name, universe), names_from = d_num, values_from = c(label, concept)) |>
    unite(col = label, starts_with("label_"), sep = "!!", na.rm = TRUE) |>
    unite(col = concept, starts_with("concept_"), sep = " by ", na.rm = TRUE) |>
    bind_rows(empty_chas) |>
    mutate(name = str_remove(name, "est"))

}
