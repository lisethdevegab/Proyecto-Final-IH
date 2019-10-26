import pandas as pd
import numpy as np

def load_data(file):
    data = pd.read_csv(file, encoding='utf8')
    df = pd.DataFrame(data)
    return df
