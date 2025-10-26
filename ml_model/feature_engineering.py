import pandas as pd

class FraudFeatureEngineer:
    """
    A feature engineering class for banking fraud detection.
    ---------------------------------------------------------
    Extracts useful features from transaction data such as:
      1. Transaction hour
      2. Day of week
      3. High-value transaction flag
      4. Total transaction count per account
      5. Average transaction amount per account
      6. Transaction velocity (within 24h)
      7. Unique device usage count per account
      8. Unique location count per account
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the feature engineer.

        Expected columns:
          - txn_id
          - account_id
          - amount
          - tx_type
          - channel
          - timestamp
          - device_id
          - location
        """
        required_cols = {
            'txn_id', 'account_id', 'amount', 'timestamp',
            'tx_type', 'channel', 'device_id', 'location'
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Ensure timestamp column is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        self.df = df.copy()

    # -------------------------------
    # Step 1: Time-based features
    # -------------------------------
    def add_time_features(self):
        """Extract hour of day and day of week."""
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        return self

    # -------------------------------
    # Step 2: High-value transactions
    # -------------------------------
    def add_high_value_flag(self):
        """Flag transactions that are unusually high value."""
        mean_amt = self.df['amount'].mean()
        self.df['is_high_value'] = (self.df['amount'] > mean_amt * 3).astype(int)
        return self

    # -------------------------------
    # Step 3: Behavioral features
    # -------------------------------
    def add_behavioral_features(self):
        """Add behavioral metrics for each account."""
        self.df['tx_count_24h'] = self.df.groupby('account_id')['txn_id'].transform('count')
        self.df['avg_tx_amount'] = self.df.groupby('account_id')['amount'].transform('mean')
        return self

    # -------------------------------
    # Step 4: Velocity (transactions in last 24 hours)
    # -------------------------------
    def add_velocity_features(self):
        """Approximate transaction velocity (number of tx in 24h window)."""
        self.df = self.df.sort_values(by=['account_id', 'timestamp'])

        # Initialize the column
        self.df['tx_24h_window'] = 0

        # Compute rolling count per account
        for acc_id, group in self.df.groupby('account_id', group_keys=False):
            # Preserve original index
            group = group.sort_values('timestamp')
            # Set timestamp as index for rolling
            group_index = group.index
            group = group.set_index('timestamp')
            group['tx_24h_window'] = group['txn_id'].rolling('1D').count()
            # Assign back using the preserved integer index
            self.df.loc[group_index, 'tx_24h_window'] = group['tx_24h_window'].values

        return self

    # -------------------------------
    # Step 5: Device and Location consistency
    # -------------------------------
    def add_device_and_location_features(self):
        """Add device and location-based behavioral features."""
        self.df['unique_devices'] = self.df.groupby('account_id')['device_id'].transform('nunique')
        self.df['unique_locations'] = self.df.groupby('account_id')['location'].transform('nunique')
        return self

    # -------------------------------
    # Build all features together
    # -------------------------------
    def build_features(self):
        """Run all feature generation steps sequentially."""
        return (
            self.add_time_features()
            .add_high_value_flag()
            .add_behavioral_features()
            .add_device_and_location_features()
            .add_velocity_features()
            .df
        )


# -------------------------------------------------------
# ✅ Example Usage
# -------------------------------------------------------
if __name__ == "__main__":
    # Sample mock data
    data = {
        'txn_id': range(1, 11),
        'account_id': [101, 101, 102, 101, 103, 102, 103, 101, 104, 104],
        'beneficiary_id': [201, 202, 203, 204, 205, 206, 207, 208, 209, 210],
        'amount': [500, 1500, 2500, 1200, 8000, 3000, 500, 25000, 400, 600],
        'tx_type': ['NEFT', 'RTGS', 'UPI', 'RTGS', 'NEFT', 'UPI', 'UPI', 'RTGS', 'NEFT', 'UPI'],
        'channel': ['Mobile', 'Web', 'ATM', 'Web', 'Mobile', 'ATM', 'Mobile', 'Web', 'ATM', 'Mobile'],
        'timestamp': pd.date_range('2025-10-26', periods=10, freq='h'),
        'device_id': ['D1', 'D1', 'D2', 'D1', 'D3', 'D2', 'D3', 'D1', 'D4', 'D4'],
        'location': ['Mumbai', 'Mumbai', 'Delhi', 'Mumbai', 'Pune', 'Delhi', 'Pune', 'Mumbai', 'Chennai', 'Chennai']
    }

    df = pd.DataFrame(data)

    # Run feature engineering
    engineer = FraudFeatureEngineer(df)
    enriched_df = engineer.build_features()

    print("✅ Final Engineered DataFrame:")
    print(enriched_df.head())
