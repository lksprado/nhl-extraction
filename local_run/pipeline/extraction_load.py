from src.extraction.controller import get_data_from_db
from src.extraction.extraction import Extractor
from endpoints import *
from config import get_local_crendentials
from src.loading.loader import Loader
from time import perf_counter
import sqlalchemy
from itertools import islice
###################################################################
## EXTRACTION
###################################################################

def all_games_details_extraction():
  """
  CONTEM DETALHES DO JOGO COM JOGADORES
  """
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_gamesid_for_games_details_tb'
  
  extractor = Extractor()
  
  config = get_all_games_details_endpoint()  
  games = get_data_from_db(engine=engine,table=table_name, cols=['game_id'])
  
  total = len(games)
  games_num = 1
  start = perf_counter()
  
  for game in games:
    print(f"Extração: {games_num} de {total}")
    url = f"https://api-web.nhle.com/v1/gamecenter/{game}/boxscore"
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.build_filename(game_id=game))
    games_num +=1

  print(f"Completed in {perf_counter() - start:2f}s")

def all_games_summary_details_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_gamesid_for_games_summary_details_tb'
  
  config = get_all_games_summary_details_endpoint()
  games = get_data_from_db(engine=engine,table=table_name, cols=['game_id'])

  total = len(games)
  games_num = 1
  start = perf_counter()
  
  for game in games:
    print(f"Extração: {games_num} de {total}")
    url = f"https://api-web.nhle.com/v1/gamecenter/{game}/right-rail"
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.build_filename(game_id=game))
    games_num +=1
  
  print(f"Completed in {perf_counter() - start:2f}s")

def all_club_stats_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_team_season_game_type_id_for_club_stats_tb'
  
  config = get_all_club_stats_endpoint()
  rows = get_data_from_db(engine=engine,table=table_name, cols=['team_id', 'season_id','game_type_id'], return_as= 'tuples')

  total = len(rows)
  start = perf_counter()

  for i, (team_id, season_id, game_type_id) in enumerate(rows, start=1):
      
      print(f"Extração: {i} de {total}")

      url = config.url.format(
          team_id=team_id,
          season_id=season_id,
          game_type_id=game_type_id
      )
      
      data = extractor.make_request(url=url)

      filename = config.filename.format(
          team_id=team_id,
          season_id=season_id,
          game_type_id=game_type_id
      )

      extractor.save_json(
          data=data,
          output_dir=config.output_dir,
          filename=filename
      )

  print(f"Completed in {perf_counter() - start:.2f}s")

def all_players_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_player_id_for_player_info'
  
  config = get_all_players_endpoint()
  players = get_data_from_db(engine=engine,table=table_name, cols=['player_id'])

  total = len(players)
  players_num = 1
  start = perf_counter()
  
  for player in players:
    print(f"Extração: {players_num} de {total}")
    url = config.url.format(player_id=player)
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.build_filename(player_id=player))
    players_num +=1
  
  print(f"Completed in {perf_counter() - start:2f}s")

###################################################################
## LOADING
###################################################################

def all_games_details_loading(*, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO COM JOGADORES
  """
  config = get_all_games_details_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  files = list(config.output_dir.glob("raw_*_details.json"))

  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]
  
  loader.load_files(config, files)

def all_games_summary_details_loading(*, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO
  """
  config = get_all_games_summary_details_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  files = list(config.output_dir.glob("raw_*_summary_details.json"))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)

def all_club_stats_loading(*, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO
  """
  config = get_all_club_stats_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  files = list(config.output_dir.glob("raw_stats_club_*_*_*.json"))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)


def all_players_loading(*, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO
  """
  config = get_all_players_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  files = list(config.output_dir.glob("player_*_info.json"))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)


if __name__ == '__main__':
  # all_games_details_extraction()
  # all_games_summary_details_extraction()
  # all_games_summary_details_loading()
  # all_games_details_loading()
  # all_club_stats_loading()
  # all_players_extraction()
  all_players_loading()
  pass