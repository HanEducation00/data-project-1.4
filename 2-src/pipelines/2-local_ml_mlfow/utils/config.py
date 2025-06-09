#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Energy Forecasting ML Pipeline Konfigürasyonu

Lokal dosyalardan veri okuyan, ML model geliştiren, MLflow'a kaydeden
ve tahminleri PostgreSQL'e yazan pipeline için konfigürasyon.
"""

# PostgreSQL konfigürasyonu
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "database": "datawarehouse",
    "user": "datauser",
    "password": "datapass",
    "driver": "org.postgresql.Driver"
}

# PostgreSQL JDBC URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# PostgreSQL bağlantı özellikleri
JDBC_PROPERTIES = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": POSTGRES_CONFIG["driver"]
}

# Tablo isimleri - Enerji tahmin modelinin verileri, performansı ve tahminleri için
TABLE_NAMES = {
    # Veri Tabloları
    "raw_data": "raw_load_data",                # Sistemdeki ham akıllı sayaç verileri
    
    # Model Tahmin ve Hata Tabloları
    "daily_predictions": "ml_daily_predictions", # Günlük enerji tahminleri ve gerçek değerler
    "prediction_errors": "ml_prediction_errors", # Tahmin hataları ve analiz verileri
    
    # Model Performans ve Analiz Tabloları
    "model_metrics": "ml_model_performance",     # Model performans metrikleri (R2, RMSE, MAE)
    "feature_importance": "ml_feature_importance", # Özellik önemlilik skorları
    
    # Model Meta Verileri
    "model_registry": "ml_model_registry"        # Eğitilen modellerin kaydı ve meta verileri
}

# Spark konfigürasyonu
SPARK_CONFIG = {
    "app_name": "System_Total_Energy_Forecasting",
    "master": "local[*]",
    "log_level": "INFO",
    "packages": "org.postgresql:postgresql:42.6.0",
    "memory": "4g",
    "driver_memory": "2g",
    "cores": "2",
    "parallelism": "8"
}

# MLflow konfigürasyonu
MLFLOW_CONFIG = {
    "tracking_uri": "http://mlflow-server:5000",
    "experiment_name": "System_Total_Energy_Forecasting",
    "model_name": "SystemEnergyForecaster",
    "artifact_path": "energy_forecasting_model",
    "local_model_path": "/workspace/models"
}

# Veri dosyaları konfigürasyonu
DATA_CONFIG = {
    "base_path": "/home/han/data/smart-ds/2016/AUS/P1R/load_timeseries",
    "alt_paths": [
        "/data/smart-ds/2016/AUS/P1R/load_timeseries",
        "/workspace/data/smart-ds/2016/AUS/P1R/load_timeseries"
    ],
    "file_pattern": "*.txt"
}

# Model hiperparametreleri
MODEL_CONFIG = {
    "algorithm": "GBTRegressor",
    "max_depth": 6,
    "max_bins": 32,
    "max_iter": 100,
    "step_size": 0.1,
    "subsampling_rate": 0.8,
    "feature_subset_strategy": "sqrt",
    "seed": 42,
    "test_days": 60
}

# Veri önişleme konfigürasyonu
PREPROCESSING_CONFIG = {
    "train_val_test_split": [0.7, 0.15, 0.15],  # Eğitim, doğrulama, test oranları
    "use_cache": True,                          # İşlenmiş verileri önbelleğe al
    "repartition_count": 4                      # Veriyi kaç partition'a böl
}

# Tablo şemaları
TABLE_SCHEMAS = {
    "daily_predictions": """
    CREATE TABLE IF NOT EXISTS ml_daily_predictions (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(50) NOT NULL,             -- MLflow run ID
        model_version VARCHAR(20) NOT NULL,      -- Model versiyonu
        date DATE NOT NULL,                      -- Tahmin tarihi
        actual_energy FLOAT,                     -- Gerçek enerji değeri
        predicted_energy FLOAT NOT NULL,         -- Tahmin edilen enerji değeri
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    
    "prediction_errors": """
    CREATE TABLE IF NOT EXISTS ml_prediction_errors (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(50) NOT NULL,             -- MLflow run ID
        model_version VARCHAR(20) NOT NULL,      -- Model versiyonu
        date DATE NOT NULL,                      -- Tahmin tarihi
        actual_energy FLOAT,                     -- Gerçek enerji değeri
        predicted_energy FLOAT NOT NULL,         -- Tahmin edilen enerji değeri
        absolute_error FLOAT NOT NULL,           -- Mutlak hata (ABS(actual - predicted))
        percentage_error FLOAT,                  -- Yüzde hata ((actual - predicted) / actual * 100)
        is_overestimation BOOLEAN,               -- Aşırı tahmin mi? (predicted > actual)
        error_category VARCHAR(20),              -- Hata kategorisi (düşük, orta, yüksek)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    
    "model_metrics": """
    CREATE TABLE IF NOT EXISTS ml_model_performance (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(50) NOT NULL,             -- MLflow run ID
        model_version VARCHAR(20) NOT NULL,      -- Model versiyonu
        train_r2 FLOAT NOT NULL,                 -- Eğitim R2 skoru
        val_r2 FLOAT NOT NULL,                   -- Doğrulama R2 skoru
        test_r2 FLOAT NOT NULL,                  -- Test R2 skoru
        train_rmse FLOAT NOT NULL,               -- Eğitim RMSE
        val_rmse FLOAT NOT NULL,                 -- Doğrulama RMSE
        test_rmse FLOAT NOT NULL,                -- Test RMSE
        train_mae FLOAT NOT NULL,                -- Eğitim MAE
        val_mae FLOAT NOT NULL,                  -- Doğrulama MAE
        test_mae FLOAT NOT NULL,                 -- Test MAE
        training_time FLOAT NOT NULL,            -- Eğitim süresi (saniye)
        total_features INT NOT NULL,             -- Toplam özellik sayısı
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    
    "feature_importance": """
    CREATE TABLE IF NOT EXISTS ml_feature_importance (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(50) NOT NULL,             -- MLflow run ID
        model_version VARCHAR(20) NOT NULL,      -- Model versiyonu
        feature_name VARCHAR(100) NOT NULL,      -- Özellik adı
        importance_score FLOAT NOT NULL,         -- Önem skoru
        importance_rank INT NOT NULL,            -- Önem sıralaması
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    
    "model_registry": """
    CREATE TABLE IF NOT EXISTS ml_model_registry (
        id SERIAL PRIMARY KEY,
        run_id VARCHAR(50) NOT NULL,             -- MLflow run ID
        model_version VARCHAR(20) NOT NULL,      -- Model versiyonu
        model_name VARCHAR(100) NOT NULL,        -- Model adı
        training_date TIMESTAMP NOT NULL,        -- Eğitim tarihi
        description TEXT,                        -- Model açıklaması
        hyperparameters JSONB,                   -- Model hiperparametreleri (JSON)
        mlflow_uri VARCHAR(255),                 -- MLflow URI
        local_path VARCHAR(255),                 -- Yerel model yolu
        is_production BOOLEAN DEFAULT FALSE,     -- Üretimde mi?
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
}

# PostgreSQL index tanımları
POSTGRES_INDEXES = {
    "daily_predictions": [
        "CREATE INDEX IF NOT EXISTS idx_daily_pred_date ON ml_daily_predictions (date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_pred_run_id ON ml_daily_predictions (run_id)"
    ],
    "prediction_errors": [
        "CREATE INDEX IF NOT EXISTS idx_pred_errors_date ON ml_prediction_errors (date)",
        "CREATE INDEX IF NOT EXISTS idx_pred_errors_run_id ON ml_prediction_errors (run_id)",
        "CREATE INDEX IF NOT EXISTS idx_pred_errors_category ON ml_prediction_errors (error_category)"
    ],
    "model_metrics": [
        "CREATE INDEX IF NOT EXISTS idx_model_metrics_run_id ON ml_model_performance (run_id)"
    ],
    "feature_importance": [
        "CREATE INDEX IF NOT EXISTS idx_feature_imp_run_id ON ml_feature_importance (run_id)",
        "CREATE INDEX IF NOT EXISTS idx_feature_imp_name ON ml_feature_importance (feature_name)",
        "CREATE INDEX IF NOT EXISTS idx_feature_imp_rank ON ml_feature_importance (importance_rank)"
    ],
    "model_registry": [
        "CREATE INDEX IF NOT EXISTS idx_model_reg_name ON ml_model_registry (model_name)",
        "CREATE INDEX IF NOT EXISTS idx_model_reg_version ON ml_model_registry (model_version)",
        "CREATE INDEX IF NOT EXISTS idx_model_reg_prod ON ml_model_registry (is_production)"
    ]
}