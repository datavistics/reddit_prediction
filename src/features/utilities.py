from pathlib import Path

import numpy as np


def get_embeddings(embeddings_path: Path):
    """
    Generate embeddings dict from pickle file if exists else from word embeddings path
    :param embeddings_path:
    :return:
    """

    embedding_vectors = {}

    # p_path = embeddings_path.with_suffix('.pickle')
    # if p_path.exists():
    #     with open(p_path, 'rb') as f:
    #         embedding_vectors = pickle.load(f)
    #     return embedding_vectors

    with open(embeddings_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_split = line.strip().split(" ")
            vec = np.array(line_split[1:], dtype=float)
            word = line_split[0]
            embedding_vectors[word] = vec

    # with open(p_path, 'wb') as f:
    #     pickle.dump(embedding_vectors, f)

    return embedding_vectors


if __name__ == '__main__':
    p = Path(__file__).parents[2]/'data'/'external'/'glove.twitter.27B.200d.txt'
    assert p.exists(), f'{str(p)} don\'t exist'
    test = get_embeddings(p)
