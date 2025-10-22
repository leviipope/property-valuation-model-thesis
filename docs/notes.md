## Feature engineering
- we will not include price_per_size because it is redudant information when predicting price and the model will reverse engineer the value and cheat on the training data.
- we will use one-hot encoding for heating type as different types of heating does not always matter in price. But will use ordinal encoding for condition beacuse a better condition will affect the price. (0 - bad, 1 - mid, 2 - good, etc.)
- we are are applying log on price, because it reduces skew and makes relationship with predictors more linear
