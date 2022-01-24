### Get endpoints name
re_type_stats <- function(url){
  return(str_extract(url, '(?<=\\/)[:alnum:]+(?=\\?)'))
}

### get endpoints
get_endpoints <- function(url){
  return(nba_params[[re_type_stats(url)]])
}

### GET function to nbastats.com
requests_nba <- function(url, count, n_rep, ...){
  argg <- c(as.list(environment()), list(...))
  param_nba <- get_endpoints(url)
  param_nba[intersect(names(argg), names(param_nba))] <- argg[intersect(names(argg), names(param_nba))]

  res <- trycatch_nbastats(url, 10, nba_request_headers, param_nba, count, n_rep, ...)
  return(res)
}


trycatch_nbastats <- function(url, t, nba_request_headers, param_nba, count, n_rep, ...){

  tryCatch({res <- GET(url = url, timeout(t), add_headers(nba_request_headers), query = param_nba)
  if(res$status_code != 200 | res$url == 'https://www.nba.com/stats/error/') {stop()}; return(res)},
           error = function(e){
             if (exists('res')){
               mes <- ' Response status is not equal to 200. Number of remaining attempts: '
             } else {
               mes <- ' No response was received from nbastats, a repeat request. Number of remaining attempts: '
             }
             if (count < n_rep){
               message(paste0(Sys.time(), mes, n_rep - count))
               Sys.sleep(2)
               return(requests_nba(url, count + 1, n_rep, ...))
             } else{
               stop(Sys.time(), ' No response was received from nbastats for ', n_rep, ' request attempts')
             }
           })
}

### GET function to pbpstats.com
requests_pbpstats <- function(url, season, team_id, game_date, count, n_rep=5, ...){

  param_poss <- list(
    TeamId = team_id,
    Season = paste0(season, '-', str_pad(as.numeric(str_sub(season, 3, 4)) + 1, 2, pad = "0")),
    SeasonType = I('Regular%2BSeason'),
    OffDef = 'Offense',
    StartType = 'All',
    FromDate = game_date,
    ToDate = game_date
  )
  
  res <- trycatch_pbpstats(url, season, 10, pbpstats_request_headers, param_poss, team_id, game_date, count, n_rep, ...)
  return(res)
}

trycatch_pbpstats <- function(url, season, t, pbpstats_request_headers, param_poss, team_id, game_date, count, n_rep, ...){

  tryCatch({res <- GET(url = url, timeout(t), add_headers(pbpstats_request_headers), query = param_poss, config = config(ssl_verifypeer = FALSE)); res},
           error = function(e){
             if (count < n_rep){
               message(paste0(Sys.time(), ' No response was received from pbpstats, a repeat request. Number of remaining attempts: ', n_rep - count))
               Sys.sleep(2)
               return(requests_pbpstats(url, season=season, team_id=team_id, game_date=game_date, count=count+1, n_rep=n_rep, ...))
             } else{
               stop(Sys.time(), ' No response was received from pbpstats for ', n_rep, ' request attempts')
             }
           })
}

### Converting time to seconds
convert_time_to_second <- function(data, column){
  column <- enquo(column)
  data %>%
    separate(!!column, c('MIN', 'SEC'), sep = ':') %>% 
    mutate_at(c('MIN', 'SEC'), as.numeric) %>% 
    mutate(!!column := ifelse(PERIOD < 5, abs((MIN * 60 + SEC) - 720 * PERIOD), abs((MIN * 60 + SEC) - (2880 + 300 * (PERIOD - 4))))) %>% 
    select(!!column) %>% 
    pull()
}

### Adding players on court
add_player_on_floor <- function(df){
  df$PERIOD <- as.numeric(df$PERIOD)
  
  df <- mutate(df, PCTIMESTRING = convert_time_to_second(df, PCTIMESTRING))
  
  all_id <- unique(c(df$PLAYER1_ID[df$EVENTMSGTYPE != 18 & !is.na(df$PLAYER1_NAME)], 
                     df$PLAYER2_ID[df$EVENTMSGTYPE != 18 & !is.na(df$PLAYER2_NAME)], 
                     df$PLAYER3_ID[df$EVENTMSGTYPE != 18 & !is.na(df$PLAYER3_NAME)]))
  
  all_id <- all_id[all_id != 0]
  
  sub_off <- unique(df$PLAYER1_ID[df$EVENTMSGTYPE == 8])
  sub_on <- unique(df$PLAYER2_ID[df$EVENTMSGTYPE == 8])
  
  setdiff(sub_on, sub_off)
  
  '%!in%' <- Negate(`%in%`)
  all_id <- all_id[all_id  %!in% setdiff(sub_on, sub_off)]
  
  sub_on_off <- intersect(sub_on, sub_off)
  
  for (i in sub_on_off){
    on <- min(df$PCTIMESTRING[df$EVENTMSGTYPE == 8 & df$PLAYER2_ID == i])
    off <- min(df$PCTIMESTRING[df$EVENTMSGTYPE == 8 & df$PLAYER1_ID == i])
    if (off > on){
      all_id <- all_id[all_id != i]
    }
  }
  
  columns <- paste0("PLAYER", seq(1, 10))
  df[columns] <- NA
  
  for(i in seq(1:10)){
    df[columns][i] <- all_id[i]
  }
  
  for(column in paste0("PLAYER", seq(1, 10))){
    i <- 1
    repeat{
      n <- nrow(df)
      if(length(which(df$EVENTMSGTYPE == 8 & df$PLAYER1_ID == df[, column])) == 0){
        break
      }
      i <- min(which(df$EVENTMSGTYPE == 8 & df[, column] == df$PLAYER1_ID))
      player_on <- df$PLAYER2_ID[i]
      df[i:n, column] <- player_on
    }
  }
  df
}

### Transformation pbpstats.com data
transform_pbp_stats <- function(data, start_period, end_period){
  data %>% 
    rename_all(toupper) %>% 
    filter(PERIOD >= start_period & PERIOD <= end_period) %>% 
    arrange(GAMEID, PERIOD, desc(STARTTIME)) %>% 
    select(GAMEID, OPPONENT, EVENTS, STARTTIME, ENDTIME, PERIOD) %>%
    distinct() %>% 
    group_by(GAMEID, OPPONENT) %>% 
    mutate(ID_POSSESSION = row_number()) %>% 
    ungroup() %>% 
    full_join(., arrange(data, GAMEID, PERIOD, desc(STARTTIME)), by = c('GAMEID', 'OPPONENT', 'EVENTS', 'STARTTIME', 'ENDTIME', 'PERIOD')) %>% 
    rename_all(toupper) %>% 
    mutate(STARTTIME = convert_time_to_second(., STARTTIME),
           ENDTIME = convert_time_to_second(., ENDTIME)) %>% 
    mutate(TIME_POSSESSION = ENDTIME - STARTTIME) %>% 
    select(-c(GAMEID, PERIOD))
}

### Converting a description to merge
convert_descritpion <- function(data, unite = FALSE){
  if(unite){
    data <- data %>%
      clear_descritpion(.)
  }
  
  data %>%
    mutate(DESCRIPTION = ifelse(str_detect(DESCRIPTION, '\\w'), DESCRIPTION, NA)) %>%
    filter(!is.na(DESCRIPTION)) %>%
    mutate(DESCRIPTION = str_trim(DESCRIPTION, 'both')) %>%
    mutate(DESCRIPTION = tolower(DESCRIPTION)) %>%
    mutate(DESCRIPTION = str_remove_all(DESCRIPTION, '[:punct:]')) %>% 
    group_by(DESCRIPTION) %>% 
    mutate(ROWS = row_number(DESCRIPTION)) %>% 
    ungroup()
}

### Delete NA from description
clear_descritpion <- function(data){
  
  data <- data %>% 
    replace_na(., list(HOMEDESCRIPTION = '', VISITORDESCRIPTION = '')) %>% 
    unite(., col = "DESCRIPTION", HOMEDESCRIPTION, VISITORDESCRIPTION, sep = " ") %>% 
    mutate(DESCRIPTION = str_trim(DESCRIPTION, side = 'both'))
  return(data)
}

### Checking count possessions
check_possessions <- function(data, season = '2019-20'){
  my_data <- data %>% 
    group_by(GAMEID, OPPONENT) %>%
    summarise(POSSESSION = max(ID_POSSESSION)) %>%
    ungroup()
  
  url <- paste0('https://api.pbpstats.com/get-games/nba?Season=', season, '&SeasonType=Regular%2BSeason')
  
  pbpstats_request_headers = c(
    "user-agent" = "Mozilla/5.0"
  )
  
  response <- GET(url, add_headers(pbpstats_request_headers))
  
  pbplist <- fromJSON(content(response, as = "text", encoding = 'UTF-8'))
  
  pbp_data <- pbplist[['results']]
  
  pbp_data <- pbp_data %>%
    select(GameId, AwayPossessions, HomeTeamAbbreviation) %>% 
    bind_rows(., pbp_data %>% select(GameId, HomePossessions, AwayTeamAbbreviation)) %>% 
    mutate(Opponent = ifelse(is.na(AwayTeamAbbreviation), HomeTeamAbbreviation, AwayTeamAbbreviation),
           Possession = ifelse(is.na(AwayPossessions), HomePossessions, AwayPossessions)) %>% 
    select(GameId, Opponent, Possession) %>% 
    mutate(across(GameId, as.numeric)) %>% 
    arrange(GameId, Opponent) %>% 
    as_tibble() %>% 
    rename_all(toupper)
  
  table <- inner_join(my_data, pbp_data, by = c('GAMEID', 'OPPONENT')) %>% 
    mutate(CHECK = POSSESSION.x == POSSESSION.y) %>% 
    filter(CHECK == FALSE)
  
  if (nrow(table) == 0){
    print('All data correctly')
  } else {
    message('Data differs from verification data')
    return(table)
  }
}

command_line_work <- function(func){
  args <- commandArgs(trailingOnly = TRUE)
  
  if(any(args %in% c('--season'))){
    season <- as.numeric(args[(which(args %in% c('--season')))+1])

    if(is.na(season)){
      season <- 2020
    }
  } else {
    season <- 2020
  }
  
  if(any(args %in% c('-s', '--start'))){
    start <- as.numeric(args[(which(args %in% c('-s', '--start')))+1])
    if(is.na(start)){
      start <- 1
    }
  } else {
    start <- 1
  }
  
  if(any(args %in% c('-e', '--end'))){
    end <- as.numeric(args[(which(args %in% c('-e', '--end')))+1])
    if(is.na(end)){
      end <- 1230
    }
  } else {
    end <- 1230
  }
  
  if(any(args %in% c('--stop'))){
    early_stop <- as.numeric(args[(which(args %in% c('--stop')))+1])
    if(is.na(early_stop)){
      early_stop <- 5
    }
  } else {
    early_stop <- 5
  }
  
  if(any(args %in% c('-v', '--verbose'))){
    verbose <- as.character(args[(which(args %in% c('-v', '--verbose')))+1])
    if(is.na(verbose)){
      verbose <- 'FALSE'
    }
  } else {
    verbose <- 'FALSE'
  }

  do.call(func, list(season, start, end, early_stop, verbose))
}