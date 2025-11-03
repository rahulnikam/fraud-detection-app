from apscheduler.schedulers.background import BackgroundScheduler
from ml_model.model_training import FraudModelTrainer

def retrain_fraud_model():
    trainer = FraudModelTrainer()
    df = trainer.load_data_from_db()
    df = trainer.preprocess_data(df)
    trainer.train_model(df)

scheduler = BackgroundScheduler()
scheduler.add_job(retrain_fraud_model, 'interval', seconds=10)
scheduler.start()

print("Scheduler started! Retraining every 10 seconds...")
while True:
    pass  # keep app alive
