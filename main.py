### Onde vai ocorrer a junção dos csv
import pandas as pd
import numpy as np
import json
import ast
import re

kabum_data = "kabum_data.csv"

kabum = pd.read_csv(kabum_data)

kabum['id'] = range(1, len(kabum) + 1)

data = {
    "product_id": kabum["id"],
    "price": kabum["price"],
    "date": pd.Timestamp.now().date()
}

price = pd.DataFrame(data)

print(price)
