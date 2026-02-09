import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy import sparse


class ItemBasedCF:
    def __init__(self, k, dist_func = cosine_similarity):
        self.k = k
        self.dist_func = dist_func
        self.n_items_row = int(np.max(self.Y_data[:, 0])) + 1
        self.n_items_col = int(np.max(self.Y_data[:, 0])) + 1