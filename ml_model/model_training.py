import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib
from imblearn.over_sampling import SMOTE

# Import your shared connection utility and feature engineering class
from scripts.db_connection import get_db_connection
from ml_model.feature_engineering import FraudFeatureEngineer
from imblearn.over_sampling import RandomOverSampler

class FraudModelTrainer:
    def __init__(self, model_path="models/random_forest.pkl"):
        # Use shared DB connection
        self.connection = get_db_connection()
        self.model_path = model_path
        # Ensure model directory exists
        os.makedirs(os.path.dirname(self.model_path) or ".", exist_ok=True)

    def load_data_from_db(self):
        """Fetch all transactions from MySQL"""
        print("📦 Fetching transactions from MySQL...")
        query = "SELECT * FROM transactions"
        df = pd.read_sql(query, self.connection)
        print(f"✅ Loaded {len(df)} records from MySQL.")
        return df

    def preprocess_data(self, df):
        """Apply feature engineering"""
        print("🧩 Applying feature engineering rules...")
        fe = FraudFeatureEngineer(df)
        df = fe.build_features()
        return df

    def train_model(self, df):
        """Train RandomForest for fraud detection"""
        print("🤖 Training RandomForest model...")

        # Extract target
        y = df['is_fraud']

        # Drop ID and timestamp
        X = df.drop(columns=['is_fraud', 'txn_id', 'timestamp'])

        # Encode categorical features
        categorical_cols = ['tx_type', 'channel', 'location', 'device_id']
        X = pd.get_dummies(X, columns=categorical_cols, drop_first=True)

        # Stratified split to ensure fraud class in test set
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Handle class imbalance (oversample only training data)
        smote = SMOTE(random_state=42)
        X_train, y_train = smote.fit_resample(X_train, y_train)

        # Train RandomForest
        model = RandomForestClassifier(
            n_estimators=100,
            random_state=42,
            class_weight='balanced'
        )
        model.fit(X_train, y_train)

        # Evaluate on test set
        preds = model.predict(X_test)
        print("📊 Model Performance Report:")
        print(classification_report(y_test, preds, zero_division=0))

        # Save model
        joblib.dump(model, self.model_path)
        print(f"✅ Model saved as {self.model_path}")

# -------------------------------------------------------
# ✅ Usage
# -------------------------------------------------------
if __name__ == "__main__":
    trainer = FraudModelTrainer()
    df = trainer.load_data_from_db()
    df = trainer.preprocess_data(df)
    trainer.train_model(df)
