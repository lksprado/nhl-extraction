from endpoints import *
from config import get_local_crendentials
from src.loading.loader import Loader


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

def all_games_gamelog_loading(*, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO
  """
  config = get_all_players_gamelog_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  files = list(config.output_dir.glob("*_*_*.json"))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)

def all_games_play_by_play_loading(*, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO
  """
  config = get_all_games_play_by_play_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  files = list(config.output_dir.glob("*raw_*.json"))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)

if __name__ == '__main__':
  # all_games_summary_details_loading()
  # all_games_details_loading()
  # all_club_stats_loading()
  # all_players_loading()
  # all_gamelog_loading()
  # all_games_play_by_play_loading()
  pass