def get_base_path():
    try:
        # Tenta pegar a variável do Airflow
        from airflow.models import Variable

        return Variable.get("lake_base_dir")
    except Exception:
        # Se falhar, assume path local (está fora do Airflow)
        return "/media/lucas/Files/2.Projetos/0.mylake/"


def get_local_crendentials()-> dict:
    # override=True para sobrescrever variáveis já existentes (ex.: USER do shell)
    from dotenv import load_dotenv
    import os
    load_dotenv()
    return {
        "host":os.getenv("DB_HOST"),
        "port":os.getenv("DB_PORT"),
        "dbname":os.getenv("DB_NAME"),
        "user":os.getenv("DB_USER"),
        "password":os.getenv("DB_PASSWORD"),
    }
