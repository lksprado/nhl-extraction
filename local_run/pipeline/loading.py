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
  
  files = list(config.output_dir.glob(config.file_pattern))

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
  
  files = list(config.output_dir.glob(config.file_pattern))
  
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
  
  files = list(config.output_dir.glob(config.file_pattern))
  
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
  
  files = list(config.output_dir.glob(config.file_pattern))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)

def all_games_gamelog_loading(*, seasons: list[str] | None = None, test_mode: bool = False):
  """
  CONTEM DETALHES DO JOGO
  - Se seasons for informado, carrega apenas essas pastas (ex.: ['20252026', '20242025']).
  - Se seasons for None, carrega a temporada mais recente (subpasta de maior nome).
  """
  config = get_all_players_gamelog_endpoint()
  creds = get_local_crendentials()
  
  loader = Loader(**creds)
  
  base_dir = config.output_dir

  if seasons:
      season_dirs = [base_dir / str(s) for s in seasons]
  else:
      subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
      if not subdirs:
          loader.logger.warning(f"No season folders found under {base_dir}")
          return
      latest = max(subdirs, key=lambda p: p.name)
      season_dirs = [latest]

  files = config.collect_files(season_dirs)
  
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
  
  files = list(config.output_dir.glob(config.file_pattern))
  
  if test_mode:
      loader.logger.info("Running in TEST MODE")
      files = files[:3]

  loader.load_files(config, files)

if __name__ == '__main__':
  all_games_summary_details_loading()
  all_games_details_loading()
  all_club_stats_loading()
  all_players_loading()
  all_games_gamelog_loading()
  all_games_play_by_play_loading()
  pass
