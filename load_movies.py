import env
from movies import load_movies

if __name__ == '__main__':
    nb_docs = load_movies(env.DATA_SOURCE, env.CLUSTER_URL)
    print(f"{nb_docs} documents indexés")
