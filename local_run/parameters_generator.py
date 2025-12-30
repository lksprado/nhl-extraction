from src.extraction.extraction import Extractor
from src.loading.loader import Loader
from config import get_local_crendentials
from endpoints import *


def get_all_seasons_id():
    config = get_all_seasons_id_endpoint()
    creds = get_local_crendentials()
    
    url = config.url
    filename = config.filename
    out_dir = config.output_dir
    
    extractor = Extractor()
    data_extracted = extractor.make_request(url)
    extractor.save_json(data=data_extracted, output_dir=out_dir, filename=filename)
    
    loader = Loader(**creds)
    
    loader.load(config)
    

def get_all_games_id():
    config = get_all_games_summary_endpoint()
    creds = get_local_crendentials()
    
    url = config.url
    filename = config.filename
    out_dir = config.output_dir
    
    extractor = Extractor()
    data_extracted = extractor.make_request(url)
    extractor.save_json(data=data_extracted, output_dir=out_dir, filename=filename)
    
    loader = Loader(**creds)
    
    loader.load(config)

def get_all_teams_id():
    config = get_all_teams_id_endpoint()
    creds = get_local_crendentials()
    
    url = config.url
    filename = config.filename
    out_dir = config.output_dir
    
    extractor = Extractor()
    data_extracted = extractor.make_request(url)
    extractor.save_json(data=data_extracted, output_dir=out_dir, filename=filename)
    
    loader = Loader(**creds)
    
    loader.load(config)

if __name__ == "__main__":
    get_all_seasons_id()
    get_all_teams_id()
    get_all_games_id()
