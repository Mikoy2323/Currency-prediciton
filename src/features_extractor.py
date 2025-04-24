from statsmodels.tsa.seasonal import STL as STL


def shift_feature(data, shifts):
    """
    Adds lagged versions of the 'value' column to the DataFrame.

    Args:
        data (pd.DataFrame): Input time series data.
        shifts (list[int]): List of lag intervals to shift by.
    """
    for shift in shifts:
        data[f"shift_{shift}"] = data["value"].shift(shift)


def mean_rolling(data, window_sizes):
    """
    Adds rolling mean features based on the 'value' column shifted by 1.

    Args:
        data (pd.DataFrame): Input time series data.
        window_sizes (list[int]): List of window sizes for rolling means.
    """
    for window in window_sizes:
        data[f"rolling_{window}_mean"] = data["value"].shift(1).rolling(window=window).mean()


def std_rolling(data, window_sizes):
    """
    Adds rolling standard deviation features based on the 'value' column shifted by 1.

    Args:
        data (pd.DataFrame): Input time series data.
        window_sizes (list[int]): List of window sizes for rolling std.
    """
    for window in window_sizes:
        data[f"rolling_{window}_std"] = data["value"].shift(1).rolling(window=window).std()


def shifted_diff(data, num_lags):
    """
    Adds lagged difference features for the 'value' column.

    Args:
        data (pd.DataFrame): Input time series data.
        num_lags (int): Number of lag differences to compute.
    """
    for i in range(1, num_lags + 1):
        data[f'value_lag_{i}'] = data["value"].diff(periods=i)


def stl_components(data):
    """
    Decomposes the 'value' series using STL and adds trend, seasonal, and residual components.

    Args:
        data (pd.DataFrame): Input time series data.
    """
    stl = STL(data["value"], period=10).fit()
    data["trend"] = stl.trend
    data["seasonal"] = stl.seasonal
    data["resid"] = stl.resid


def expanding_window(data):
    """
    Adds expanding mean of the 'value' column (shifted by 1).

    Args:
        data (pd.DataFrame): Input time series data.
    """
    data[f"expanding_mean_1"] = data["value"].shift(1).expanding().mean()


def prepare_features(data):
    """
    Applies all feature extraction steps to the input DataFrame.

    Args:
        data (pd.DataFrame): Input time series data.

    Returns:
        pd.DataFrame: DataFrame with extracted features.
    """
    shift_feature(data, [1, 2])
    mean_rolling(data, [2, 3])
    std_rolling(data, [2, 3])
    shifted_diff(data, 2)
    stl_components(data)
    expanding_window(data)
    return data
