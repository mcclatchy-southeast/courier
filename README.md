
<!-- README.md is generated from README.Rmd. Please edit that file -->

                             _          
      _________  __  _______(_)__  _____
     / ___/ __ \/ / / / ___/ / _ \/ ___/
    / /__/ /_/ / /_/ / /  / /  __/ /    
    \___/\____/\__,_/_/  /_/\___/_/     
                                        

<!-- badges: start -->
<!-- badges: end -->

Courier is an R package providing a suite of tools for generating
placekeys using the bulk data API and progress reporting via Slack bots.
A work in progress.

## Installation

You can install the development version of `courier` from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("mcclatchy-southeast/courier")
```

## Usage

After installation, you can load the library like any other package.

``` r
##load courier package
library(courier)
```

Create a dataframe with 10,000+ addresses to submit.

``` r
#create a test dataframe
loc_test <- dplyr::tibble(
  location_name = rep(NA, 7),
  street_address = c('421 Fayetteville St', '2619 Western Blvd', '319 Fayetteville St',
                    '1205 Front Street', '2505 Atlantic Ave', '827 W Morgan St',
                    '126 E Cabarrus St'),
  city = rep('Raleigh', 7),
  region = rep('North Carolina', 7),
  postal_code = c("27601", '27606', '27601', '27609','27604', '27603', '27601'),
  iso_country_code = rep('US', 7)
  ) %>%
  dplyr::slice(rep(1:dplyr::n(), each = 1443))
```

Use the bulk uploader with `dplyr`, making sure to set your API key in
an environmental variable. Obtain a key from <https://www.placekey.io/>.

``` r
#set env variable with placekey API key
Sys.setenv(PLACEKEY_SECRET = '<<YOUR-PLACEKEY-API-KEY-HERE>>')

#Hit the API in bulk and return placekeys
loc_test_pk <- loc_test %>%
  dplyr::mutate(placekey = get_placekeys(
    location_name,
    street_address,
    city,
    region,
    postal_code,
    iso_country_code)
    )
```

<!--You'll still need to render `README.Rmd` regularly, to keep `README.md` up-to-date. `devtools::build_readme()` is handy for this. You could also use GitHub Actions to re-render `README.Rmd` every time you push. An example workflow can be found here: <https://github.com/r-lib/actions/tree/v1/examples>.-->
