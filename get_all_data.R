#! /opt/R/4.0.2/bin/Rscript --vanilla

library(httr, warn.conflicts = FALSE)
library(jsonlite, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

source('helpers.R')
source('utils.R')

get_nba_data <- function(GameID, season){
  GameID <- paste0(paste0('002', str_sub(season, 3, 4), '0'), str_pad(GameID, 4, side = "left", pad = 0))
  url <- paste0("https://data.nba.com/data/v2015/json/mobile_teams/nba/", season, "/scores/pbp/", GameID, "_full_pbp.json")

  count <- 1
  response <- trycatch_datanba(url, 10, nba_request_headers, count, 5)
  json <- fromJSON(content(response, as = "text", encoding = 'UTF-8'))

  game_id <- json$g$gid
  period <- json$g$pd$p

  data <- bind_rows(lapply(period, function(period){mutate(json$g$pd$pla[[period]], PERIOD = period)}))
  data <- mutate(data, GAME_ID = game_id)

  return(data)
}

get_pbp_stats <- function(GameID, season, ...){

  ### Get game date
  if (season < 2000){
    message('Statistics on pbpstats.com start from 2000/01 season')
    return(NULL)
  }
  game_summary <- get_boxscore_summary(GameID, season, headers = 'GameSummary')
  game_date <- format(as.Date(game_summary$GAME_DATE_EST[1],format="%Y-%m-%d"))
  team_id <- c(game_summary$HOME_TEAM_ID[1], game_summary$VISITOR_TEAM_ID[1])
  start_period <- 1
  end_period <- game_summary$LIVE_PERIOD[1]

  ### Get offense possessions teams
  count <- 1
  url <- 'https://api.pbpstats.com/get-possessions/nba'

  response <- lapply(team_id, requests_pbpstats, url = url, season = season, game_date = game_date, count = count, n_rep = 5)

  pbp_data <- bind_rows(lapply(response, function(x){
    json <- fromJSON(content(x, as = "text", encoding = 'UTF-8'))
    pbp_data <- json[['possessions']]

    pbp_data <- pbp_data %>%
      unnest(., VideoUrls, keep_empty = TRUE) %>%
      rename_all(toupper)
  }))

  return(pbp_data)
}

get_nba_pbp <- function(GameID, season, player_on_floor = FALSE, ...){
  GameID <- paste0(paste0('002', str_sub(season, 3, 4), '0'), str_pad(GameID, 4, side = "left", pad = 0))
  url <- 'https://stats.nba.com/stats/playbyplayv2?'
  
  ## application request counter
  count <- 1

  response <- requests_nba(url, count, 5, GameID = GameID, ...)
  json <- fromJSON(content(response, as = "text"))

  nba_data <- tryCatch({data.frame(matrix(unlist(json$resultSets$rowSet[[1]]), ncol = length(json$resultSets$headers[[1]]), byrow = FALSE))}, error = function(e) return(NULL))
  if(is.null(nba_data)){
    return(NULL)
  }
  names(nba_data) <- json$resultSets$headers[[1]]

  if (player_on_floor){
    nba_data <- purrr::map_dfr(nba_data %>% group_split(), add_player_on_floor)
  }
  return(nba_data)
}

get_boxscore_summary <- function(GameID, season, headers = c('all', "GameSummary", "OtherStats", "Officials", "InactivePlayers", "GameInfo", "LineScore", "LastMeeting", "SeasonSeries", "AvailableVideo")){
  match.arg(headers)
  GameID <- paste0(paste0('002', str_sub(season, 3, 4), '0'), str_pad(GameID, 4, side = "left", pad = 0))
  url <- 'https://stats.nba.com/stats/boxscoresummaryv2?'
  
  count <- 1
  response <- requests_nba(url, count, 5, GameID = GameID)
  json <- fromJSON(content(response, as = "text"))
  if (headers == 'all'){
    l <- lapply(seq_along(json$resultSets$name), function(x){
      dt <-data.frame(matrix(unlist(json$resultSets$rowSet[[x]]), ncol = length(json$resultSets$headers[[1]]), byrow = FALSE))
      colnames(dt) <- json$resultSets$headers[[x]]
      return(dt)
    })
    names(l) <- json$resultSets$name
    return(l)
  } else {
    n <- which(json$resultSets$name == headers)
    df <- data.frame(matrix(unlist(json$resultSets$rowSet[[n]]), ncol = length(json$resultSets$headers[[1]]), byrow = FALSE))
    colnames(df) <- json$resultSets$headers[[n]]
    return(df)
  }
}

get_season_pbp_full <- function(season, start=1, end=1230, early_stop = 5, verbose='FALSE'){

  if (!dir.exists(suppressWarnings(normalizePath(paste0('datasets/', season))))){
    dir.create(suppressWarnings(normalizePath(paste0('datasets/', season))), recursive = TRUE)
  }

  if (!dir.exists(suppressWarnings(normalizePath(paste0('datasets/', season, '/nbastats'))))){
    dir.create(suppressWarnings(normalizePath(paste0('datasets/', season, '/nbastats'))), recursive = TRUE)
  }

  if (season >= 2000){
    if (!dir.exists(suppressWarnings(normalizePath(paste0('datasets/', season, '/pbpstats'))))){
      dir.create(suppressWarnings(normalizePath(paste0('datasets/', season, '/pbpstats'))), recursive = TRUE)
    }
  }

  if (season >= 2016){
    if (!dir.exists(suppressWarnings(normalizePath(paste0('datasets/', season, '/datanba'))))){
      dir.create(suppressWarnings(normalizePath(paste0('datasets/', season, '/datanba'))), recursive = TRUE)
    }
  }

  early_st <- 0
  sleep <- 1
  for (i in seq(start, end)){

    exists_nbastats <- as.integer(!file.exists(suppressWarnings(normalizePath(paste('./datasets', season, '/nbastats', paste0(paste(season, i, sep = '_'), '.csv'), sep = '/')))))
    exists_pbpstats <- as.integer(!file.exists(suppressWarnings(normalizePath(paste('./datasets', season, '/pbpstats', paste0(paste(season, i, sep = '_'), '.csv'), sep = '/')))))
    exists_nbadata <- as.integer(!file.exists(suppressWarnings(normalizePath(paste('./datasets', season, '/datanba', paste0(paste(season, i, sep = '_'), '.csv'), sep = '/')))))

    if(sum(c(exists_nbastats, exists_pbpstats, exists_nbadata)) == 0){
      next
    } else {
      if (sleep %% 100 == 0){
        Sys.sleep(600)
      }

      sleep <- sleep + 1
      for(n in c("get_nba_pbp"[exists_nbastats], "get_pbp_stats"[exists_pbpstats], "get_nba_data"[exists_nbadata])){
        if(season < 2000){
          if(n %in% c("get_pbp_stats", "get_nba_data")){
            next
          }
        } else if(season < 2016){
          if(n == "get_nba_data"){
            next
          }
        }

        dt <- do.call(n, list(GameID = i, season = season))

        if(n == "get_nba_pbp"){
          if (is.null(dt)){
            early_st <- early_st + 1
            if (early_st >= early_stop){
              break
            } else {
              Sys.sleep(5)
              next
            }
          }
        }
        early_st <- 0
        folder <- switch(n,
                         "get_nba_pbp" = "nbastats",
                         "get_pbp_stats" = "pbpstats",
                         "get_nba_data" = "datanba")

        write.csv(dt, file = suppressWarnings(normalizePath(paste('./datasets',  season, '/', folder, paste0(paste(season, i, sep = '_'), '.csv'), sep = '/'))), 
                  row.names = FALSE)
      }
    }

    if (as.logical(verbose)){
      print(paste('Файл',  paste0(paste(season, i, sep = '_'), '.csv'), 'сохранён в папке', normalizePath(paste('./datasets', season, sep = '/'))))
    }
    Sys.sleep(5)
  }
}

get_season_pbp_full(2020, start = 1, end = 3)

command_line_work(get_season_pbp_full)