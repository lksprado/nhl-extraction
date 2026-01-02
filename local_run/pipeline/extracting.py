from src.extraction.controller import get_data_from_db
from src.extraction.extraction import Extractor
from endpoints import *
from time import perf_counter
import sqlalchemy


###################################################################
## EXTRACTION
###################################################################

def all_games_details_extraction():
  """
  CONTEM DETALHES DO JOGO COM JOGADORES
  """
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_request_games_id'
  
  extractor = Extractor()
  
  config = get_all_games_details_endpoint()  
  games = get_data_from_db(engine=engine,table=table_name, cols=['game_id'], bool_filter=('has_games_details', False))
  
  total = len(games)
  games_num = 1
  start = perf_counter()
  
  for game in games:
    print(f"Extração: {games_num} de {total}")
    url = config.url.format(game_id = game)
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.filename.format(game_id=game))
    games_num +=1

  print(f"Completed in {perf_counter() - start:2f}s")

def all_games_summary_details_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_request_games_id'
  
  config = get_all_games_summary_details_endpoint()
  games = get_data_from_db(engine=engine,table=table_name, cols=['game_id'], bool_filter=('has_games_summary_details', False))

  total = len(games)
  games_num = 1
  start = perf_counter()
  
  for game in games:
    print(f"Extração: {games_num} de {total}")
    url = config.url.format(game_id = game)
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.filename.format(game_id=game))
    games_num +=1
  
  print(f"Completed in {perf_counter() - start:2f}s")

def all_club_stats_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_request_teams_seasons_gametypes_id'
  
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
  table_name = 'vw_stg_request_players_id'
  
  config = get_all_players_endpoint()
  players = get_data_from_db(engine=engine,table=table_name, cols=['player_id'])

  total = len(players)
  players_num = 1
  start = perf_counter()
  
  for player in players:
    print(f"Extração: {players_num} de {total}")
    url = config.url.format(player_id=player)
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.filename.format(player_id=player))
    players_num +=1
  
  print(f"Completed in {perf_counter() - start:2f}s")

def all_games_gamelog_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_request_players_seasons_gametypes_id'
  
  config = get_all_players_gamelog_endpoint()
  rows = get_data_from_db(engine=engine,table=table_name, cols=['player_id', 'season_id','game_type_id'], return_as= 'tuples')

  total = len(rows)
  start = perf_counter()

  for i, (player_id, season_id, game_type_id) in enumerate(rows, start=1):
      
      print(f"Extração: {i} de {total}")

      url = config.url.format(
          player_id=player_id,
          season_id=season_id,
          game_type_id=game_type_id
      )
      
      data = extractor.make_request(url=url)

      filename = config.filename.format(
          player_id=player_id,
          season_id=season_id,
          game_type_id=game_type_id
      )

      output_dir = config.output_dir / str(season_id)

      extractor.save_json(
          data=data,
          output_dir=output_dir,
          filename=filename
      )

  print(f"Completed in {perf_counter() - start:.2f}s")

def all_games_play_by_play_extraction():
  """
  CONTEM DETALHES DO JOGO
  """
  extractor = Extractor()
  
  engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:pg12345@localhost:5435/postgres')
  table_name = 'vw_stg_request_games_id'
  
  config = get_all_games_play_by_play_endpoint()
  games = get_data_from_db(engine=engine,table=table_name, cols=['game_id'], bool_filter=("has_play_by_play",False))

  total = len(games)
  games_num = 1
  start = perf_counter()
  
  for game in games:
    print(f"Extração: {games_num} de {total}")
    url = config.url.format(game_id = game)
    data = extractor.make_request(url=url)
    extractor.save_json(data=data, output_dir=config.output_dir, filename=config.filename.format(game_id=game))
    games_num +=1
  
  print(f"Completed in {perf_counter() - start:2f}s")


if __name__ == '__main__':
  all_games_details_extraction()
  all_games_summary_details_extraction()
  all_club_stats_extraction()
  # all_players_extraction()
  all_games_gamelog_extraction()
  all_games_play_by_play_extraction()
  pass
