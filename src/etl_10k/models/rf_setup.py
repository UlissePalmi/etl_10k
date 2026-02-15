import pandas as pd



def X_y_builder(df):
    """
    Build the feature matrix `X` and target vector `y` for modeling.

    Selects the feature columns, extracts `prediction` as the target,
    and drops rows with missing values.
    """  
    feature_cols = df.columns[3:45]

    X = df[feature_cols]
    y = df["prediction"]

    mask = X.notna().all(axis=1) & y.notna()
    X = X[mask]
    y = y[mask]
    return X, y
