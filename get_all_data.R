#! /opt/R/4.0.2/bin/Rscript --vanilla

library(httr, warn.conflicts = FALSE)
library(jsonlite, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(tidyr, warn.conflicts = FALSE)
library(stringr, warn.conflicts = FALSE)

source('helpers.R')
source('utils.R')


get_pbp_stats <- function(GameID, season, player_on_floor = FALSE, possesions = FALSE, full_data = FALSE, ...){

  nba_data <- get_nba_pbp(GameID, season, player_on_floor, ...)

  if(is.null(dim(nba_data))){
    return(nba_data)
  }

  if(possesions){
    ### Get game date
    if (season < 2000){
      message('Statistics on pbpstats.com start from 2000/01 season')
      return(nba_data)
    }
    game_date <- get_boxscore_summary(GameID, season, headers = 'GameSummary')
    game_date <- format(as.Date(game_date$GAME_DATE_EST[1],format="%Y-%m-%d"))
    team_id <- unique(nba_data$PLAYER1_TEAM_ID)
    team_id <- team_id[!is.na(team_id)]
    start_period <- min(nba_data$PERIOD)
    end_period <- max(nba_data$PERIOD)

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

    ### Data transformation
    pbp_data <- transform_pbp_stats(pbp_data, start_period, end_period)
    
    ### Merge nbastats and pbpstats data
    nba_data_merge <- nba_data %>% select(EVENTNUM, HOMEDESCRIPTION, VISITORDESCRIPTION)

    if (!full_data){
      pbp_data <- pbp_data %>% select(STARTTIME, ENDTIME, ID_POSSESSION, DESCRIPTION,
                                      STARTSCOREDIFFERENTIAL, STARTTYPE, URL)
    }

    nba_data_merge <- convert_descritpion(nba_data_merge, TRUE)
    pbp_data <- convert_descritpion(pbp_data, FALSE)

    nba_data_merge <- left_join(nba_data_merge, pbp_data, by = c("DESCRIPTION", "ROWS"))
    if (!full_data){
      nba_data_merge <- nba_data_merge %>%  select(-DESCRIPTION)
    }
    nba_data <- left_join(nba_data, nba_data_merge, by = 'EVENTNUM')
  }
  return(nba_data)
}

get_nba_pbp <- function(GameID, season, player_on_floor = FALSE, ...){
  GameID <- paste0(paste0('002', str_sub(season, 3, 4), '0'), str_pad(GameID, 4, side = "left", pad = 0))
  url <- 'https://stats.nba.com/stats/playbyplayv2?'
  
  ## application request counter
  count <- 1

  response <- requests_nba(url, count, 5, GameID = GameID, ...)
  json <- fromJSON(content(response, as = "text"))
  nba_data <- data.frame(matrix(unlist(json$resultSets$rowSet[[1]]), ncol = length(json$resultSets$headers[[1]]), byrow = FALSE))
  tryCatch({names(nba_data) <- json$resultSets$headers[[1]]}, error = function(e) return(nba_data))

  if(sum(dim(nba_data)) == 0){
    return(NA)
  }

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
  early_st <- 0
  for (i in seq(start, end)){
    
    if (file.exists(suppressWarnings(normalizePath(paste('./datasets', season, paste0(paste(season, i, sep = '_'), '.csv'), sep = '/'))))){
      next
    }

    if (i %% 100 == 0){
      Sys.sleep(600)
    }
    t <- get_pbp_stats(i, season, player_on_floor = TRUE, possesions = TRUE, full_data = TRUE)

    if (is.null(dim(t))){
      early_st <- early_st + 1
      if (early_st >= early_stop){
        break
      } else {
        Sys.sleep(5)
        next
      }
    }
    early_st <- 0

    write.csv(t, file = suppressWarnings(normalizePath(paste('./datasets', season, paste0(paste(season, i, sep = '_'), '.csv'), sep = '/'))), 
              row.names = FALSE)
    
    if (as.logical(verbose)){
      print(paste('Файл',  paste0(paste(season, i, sep = '_'), '.csv'), 'сохранён в папке', normalizePath(paste('./datasets', season, sep = '/'))))
    }
    Sys.sleep(5)
  }
}

command_line_work(get_season_pbp_full)