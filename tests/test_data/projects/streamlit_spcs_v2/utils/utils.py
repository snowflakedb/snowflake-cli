import numpy as np
import pandas as pd


def hello_world():
    return "Hello from SPCS utils!"


def generate_sample_data():
    """Generate sample data using numpy and pandas for demonstration"""
    # Generate random data using numpy
    np.random.seed(42)  # For reproducible results
    dates = pd.date_range("2024-01-01", periods=100, freq="D")

    # Generate some sample time series data
    base_values = np.random.randn(100).cumsum() + 100
    temperature = base_values + np.random.normal(0, 5, 100)
    humidity = np.clip(np.random.normal(60, 15, 100), 0, 100)

    # Create DataFrame
    df = pd.DataFrame(
        {
            "date": dates,
            "temperature": temperature,
            "humidity": humidity,
            "pressure": np.random.normal(1013, 20, 100),
        }
    )

    return df.set_index("date")


def calculate_moving_average(data, window=7):
    """Calculate moving average using pandas"""
    return data.rolling(window=window).mean()
