import src.database as db
from pathlib import Path
import joblib
from src.features_extractor import prepare_features
import pandas as pd
from datetime import datetime, timedelta

MODEL_DIR = Path.cwd() / "models"


def retrain_and_save_models(models):
    """
    Retrains each model using the latest historical exchange rate data
    and saves the updated models to disk.

    Args:
        models (dict): A dictionary where keys are currency IDs and values are scikit-learn model instances.
    """
    for currency_id, model in models.items():
        prev_values = db.return_values("ExchangeRates",
                                       col="date, value",
                                       cond=f"WHERE currency_id = {currency_id}")
        currency_data = pd.DataFrame(prev_values, columns=["date", "value"])
        currency_data = prepare_features(currency_data)
        X = currency_data.drop(['value','date'], axis=1).values
        y = currency_data['value'].values
        model.fit(X, y)
        joblib.dump(model, MODEL_DIR / f'model_{currency_id}.joblib')


def predict_values(data, models):
    """
    Predicts the next exchange rate for each currency using the most recent data
    and the corresponding pre-trained model.

    Args:
        data (pd.DataFrame): DataFrame containing the latest exchange rate entries.
        models (dict): A dictionary where keys are currency IDs and values are loaded model instances.

    Returns:
        pd.DataFrame: DataFrame containing currency IDs and their corresponding predicted values.
    """
    predictions = []
    for i in range(len(data)):
        currency_id = i
        prev_values = db.return_values("ExchangeRates",
                                       col="date, value",
                                       cond=f"WHERE currency_id = {currency_id}")
        currency_data = pd.DataFrame(prev_values, columns=["date", "value"])
        currency_data = prepare_features(currency_data)

        currency_data.index = pd.to_datetime(currency_data["date"])
        X = currency_data.drop(['value', 'date'], axis=1).values
        if len(X) > 0:
            prediction = models[currency_id].predict(X[-1].reshape(1, -1))[0]
            predictions.append((currency_id, prediction))

    currency_ids = [i[0] for i in predictions]
    predictions = [i[1] for i in predictions]
    prediction_df = pd.DataFrame({"currency_id": currency_ids, "prediction": predictions})
    return prediction_df


def main():
    """
    Main pipeline function:
    - Loads today's exchange rate data from the database.
    - Loads pre-trained models for each currency.
    - Predicts tomorrow’s exchange rates and inserts them into the database.
    - Retrains models using the most recent data and saves them.
    """
    currency_df = db.exchange_rates_insert()
    date = currency_df.loc[0, "date"]
    models = {}
    for i in range(len(currency_df)):
        currency_id = i
        model_path = MODEL_DIR / f'model_{i}.joblib'
        models[currency_id] = joblib.load(model_path)

    # predictions for tommorow
    prediction_df = predict_values(currency_df, models)
    tomorrow = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    db.predictions_insert(prediction_df, tomorrow)
    retrain_and_save_models(models)


if __name__ == "__main__":
    main()



