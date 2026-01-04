
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable
from .config import get_base_path

@dataclass
class EndpointConfig:
  url: str
  filename: str
  output_dir: Path
  table_name: str
  is_overwrite: bool = False
  search_recursive: bool = False
  file_pattern: str = None
  schema: str = 'raw'
  array_key: Optional[str] = None 

  def collect_files(self, subdirs: Iterable[Path] | None = None) -> list[Path]:
      roots = list(subdirs) if subdirs else [self.output_dir]
      pattern = self.file_pattern or "*"
      files: list[Path] = []
      for root in roots:
          files.extend(root.rglob(pattern) if self.search_recursive else root.glob(pattern))
      return files

## STATIC ################################################################################

def get_all_seasons_id_endpoint() -> EndpointConfig:

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/single/'
  
  return EndpointConfig(
      url="https://api-web.nhle.com/v1/season",
      filename="all_season_ids.json",
      output_dir=output_path,
      table_name="nhl_raw_all_seasons_id",
      is_overwrite=True
  )

def get_all_teams_id_endpoint() -> EndpointConfig:
  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/single/'

  return EndpointConfig(
      url="https://api.nhle.com/stats/rest/en/team",
      filename="all_teams_ids.json",
      output_dir=output_path,
      array_key='data',
      table_name="nhl_raw_all_teams_id",
      is_overwrite=True
  )

def get_all_games_summary_endpoint() -> EndpointConfig:

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/single/'
  
  return EndpointConfig(
    url="https://api.nhle.com/stats/rest/en/game",
    filename="all_games_summary.json",
    output_dir=output_path,
    array_key="data",
    table_name="nhl_raw_all_games_summary",
    is_overwrite=True
  )

## DYNAMIC ################################################################################

def get_all_games_details_endpoint() -> EndpointConfig:
  """
  CONTEM DETALHES DO JOGO COM JOGADORES
  """

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/raw_all_games_details'

  return EndpointConfig(
    url="https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore",
    filename="raw_{game_id}_details.json",
    output_dir=output_path,
    table_name="nhl_raw_all_games_details",
    file_pattern="raw_*_details.json",
    is_overwrite=False
  )

def get_all_games_summary_details_endpoint() -> EndpointConfig:
  """
  CONTEM DETALHES DO JOGO 
  """

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/raw_all_games_summary_details'

  return EndpointConfig(
    url="https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail",
    filename="raw_{game_id}_summary_details.json",
    output_dir=output_path,
    table_name="nhl_raw_all_games_summary_details",
    file_pattern="raw_*_summary_details.json",
    is_overwrite=False
  )

def get_all_club_stats_endpoint() -> EndpointConfig:
  """
  CONTEM ESTATISTICAS DOS JOGADORES NO TIME POR TEMPORADA
  """

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/raw_club_stats'

  return EndpointConfig(
    url="https://api-web.nhle.com/v1/club-stats/{team_id}/{season_id}/{game_type_id}",
    filename="raw_stats_club_{team_id}_{season_id}_{game_type_id}.json",
    output_dir=output_path,
    table_name="nhl_raw_all_club_stats",
    file_pattern="raw_stats_club_*_*_*.json",
    is_overwrite=True
  )

def get_all_players_endpoint() -> EndpointConfig:
  """
  CONTEM DETALHES DOS JOGADORES
  """

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/raw_player_info'

  return EndpointConfig(
    url="https://api-web.nhle.com/v1/player/{player_id}/landing",
    filename="player_{player_id}_info.json",
    output_dir=output_path,
    table_name="nhl_raw_all_players",
    file_pattern="player_*_info.json",
    is_overwrite=True
  )

def get_all_players_gamelog_endpoint() -> EndpointConfig:
  """
  CONTEM DETALHES DOS JOGADORES
  """

  BASE_PATH = Path(get_base_path())
  output_dir = BASE_PATH / "raw/nhl/raw_game_log"

  return EndpointConfig(
    url="https://api-web.nhle.com/v1/player/{player_id}/game-log/{season_id}/{game_type_id}",
    filename="{player_id}_{season_id}_{game_type_id}.json",
    output_dir=output_dir,
    table_name="nhl_raw_all_player_game_log",
    file_pattern="*_*_*.json",
    search_recursive=True,
    is_overwrite=True
  )

def get_all_games_play_by_play_endpoint() -> EndpointConfig:
  """
  CONTEM DETALHES DOS JOGADORES
  """

  BASE_PATH = Path(get_base_path())
  output_path = BASE_PATH / 'raw/nhl/raw_play_by_play'

  return EndpointConfig(
    url="https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play",
    filename="raw_{game_id}.json",
    output_dir=output_path,
    table_name="nhl_raw_all_play_by_play",
    file_pattern="raw_*.json",
    is_overwrite=False
  )
