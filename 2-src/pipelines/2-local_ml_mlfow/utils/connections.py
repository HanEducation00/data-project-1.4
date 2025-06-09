#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database, Spark ve MLflow Bağlantı Fonksiyonları
Bu modül PostgreSQL, Spark ve MLflow bağlantılarını yönetir.
"""

import os
import json
import mlflow
import psycopg2
from psycopg2.extras import Json
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, abs  # ✅ when ve abs eklendi
from pyspark.sql.types import DateType  # ✅ DateType eklendi
from .config import (
    POSTGRES_CONFIG, SPARK_CONFIG, MLFLOW_CONFIG, 
    JDBC_URL, JDBC_PROPERTIES, TABLE_NAMES, TABLE_SCHEMAS, POSTGRES_INDEXES
)
from utils.logger import get_logger


logger = get_logger(__name__)

# Global Spark session
_spark_session = None

def get_spark_session():
    """
    Spark session oluştur ve döndür
    
    Returns:
        SparkSession: Konfigüre edilmiş Spark session
    """
    global _spark_session
    
    if _spark_session is not None:
        return _spark_session
    
    logger.info("Spark session oluşturuluyor...")
    
    try:
        # Spark builder yapılandırması
        builder = SparkSession.builder \
            .appName(SPARK_CONFIG["app_name"]) \
            .config("spark.jars.packages", SPARK_CONFIG["packages"]) \
            .config("spark.executor.memory", SPARK_CONFIG["memory"]) \
            .config("spark.driver.memory", SPARK_CONFIG["driver_memory"]) \
            .config("spark.executor.cores", SPARK_CONFIG["cores"]) \
            .config("spark.default.parallelism", SPARK_CONFIG["parallelism"]) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .master(SPARK_CONFIG["master"])
        
        # Session oluştur
        _spark_session = builder.getOrCreate()
        
        # Log seviyesini ayarla
        _spark_session.sparkContext.setLogLevel(SPARK_CONFIG["log_level"])
        
        logger.info(f"✅ Spark session başarıyla oluşturuldu (v{_spark_session.version})")
        
        return _spark_session
        
    except Exception as e:
        logger.error(f"❌ Spark session oluşturma hatası: {e}")
        raise e

def stop_spark_session():
    """Spark session'ı kapat"""
    global _spark_session
    
    if _spark_session is not None:
        logger.info("Spark session kapatılıyor...")
        _spark_session.stop()
        _spark_session = None
        logger.info("✅ Spark session kapatıldı")

def setup_mlflow():
    """
    MLflow izleme ve deney ortamını kur
    
    Returns:
        tuple: (tracking_uri, experiment_name, experiment_id)
    """
    logger.info("MLflow ayarlanıyor...")
    
    tracking_uri = MLFLOW_CONFIG["tracking_uri"]
    experiment_name = MLFLOW_CONFIG["experiment_name"]
    
    mlflow.set_tracking_uri(tracking_uri)
    
    try:
        # Deney oluştur veya mevcut deneyi al
        experiment_id = mlflow.create_experiment(experiment_name)
        logger.info(f"✅ Yeni deney oluşturuldu: {experiment_name}")
    except:
        # Deney zaten varsa
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
        logger.info(f"✅ Mevcut deney kullanılıyor: {experiment_name}")
    
    mlflow.set_experiment(experiment_name)
    
    logger.info(f"📁 MLflow tracking URI: {tracking_uri}")
    logger.info(f"🧪 Deney: {experiment_name}")
    logger.info(f"🆔 Deney ID: {experiment_id}")
    
    return tracking_uri, experiment_name, experiment_id

def test_postgresql_connection():
    """
    PostgreSQL bağlantısını test et
    
    Returns:
        bool: Bağlantı başarılı ise True
    """
    try:
        logger.info("PostgreSQL bağlantısı test ediliyor...")
        
        spark = get_spark_session()
        test_df = spark.read.jdbc(
            url=JDBC_URL,
            table="(SELECT 1 as test) AS test_query",
            properties=JDBC_PROPERTIES
        )
        
        # Test sorgusu çalıştır
        test_df.collect()
        
        logger.info("✅ PostgreSQL bağlantısı başarılı!")
        return True
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL bağlantı hatası: {e}")
        return False

def table_exists(table_name):
    """
    Tablo var mı kontrol et
    
    Args:
        table_name (str): Tablo adı
        
    Returns:
        bool: Tablo varsa True
    """
    try:
        spark = get_spark_session()
        tables_df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}') AS tables",
            properties=JDBC_PROPERTIES
        )
        
        return tables_df.count() > 0
        
    except Exception as e:
        logger.error(f"❌ Tablo kontrol hatası: {e}")
        return False

def create_table_if_not_exists(table_type):
    """
    Belirtilen tablo tipinde tablo oluştur
    
    Args:
        table_type (str): TABLE_SCHEMAS içindeki tablo tipi
        
    Returns:
        bool: Başarılı ise True
    """
    if table_type not in TABLE_SCHEMAS:
        logger.error(f"❌ Bilinmeyen tablo tipi: {table_type}")
        return False
    
    table_name = TABLE_NAMES.get(table_type)
    
    if not table_name:
        logger.error(f"❌ '{table_type}' için tablo adı bulunamadı")
        return False
    
    if table_exists(table_name):
        logger.info(f"✅ Tablo zaten mevcut: {table_name}")
        return True
    
    try:
        logger.info(f"📝 Tablo oluşturuluyor: {table_name}")
        
        # psycopg2 ile direkt PostgreSQL bağlantısı
        import psycopg2
        
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["host"],
            port=POSTGRES_CONFIG["port"],
            database=POSTGRES_CONFIG["database"],
            user=POSTGRES_CONFIG["user"],
            password=POSTGRES_CONFIG["password"]
        )
        
        cursor = conn.cursor()
        
        # Tabloyu oluştur
        table_schema = TABLE_SCHEMAS[table_type]
        sql_statements = table_schema.strip().split(';')
        
        for sql in sql_statements:
            sql = sql.strip()
            if sql and not sql.startswith('--'):  # Boş ve yorum satırlarını atla
                logger.debug(f"SQL çalıştırılıyor: {sql[:50]}...")
                cursor.execute(sql)
        
        # İndeksleri oluştur
        if table_type in POSTGRES_INDEXES:
            for index_sql in POSTGRES_INDEXES[table_type]:
                try:
                    logger.info(f"İndeks oluşturuluyor: {index_sql[:50]}...")
                    cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"⚠️ İndeks oluşturma hatası (devam ediliyor): {e}")
        
        # Değişiklikleri kaydet
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Tablo başarıyla oluşturuldu: {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Tablo oluşturma hatası: {e}")
        return False

def create_all_tables():
    """
    Tüm tabloları oluştur
    
    Returns:
        bool: Tüm tablolar başarıyla oluşturulduysa True
    """
    logger.info("📊 Tüm tabloları oluşturma başlatılıyor...")
    
    success = True
    for table_type in TABLE_SCHEMAS.keys():
        if not create_table_if_not_exists(table_type):
            success = False
    
    if success:
        logger.info("✅ Tüm tablolar başarıyla oluşturuldu")
    else:
        logger.warning("⚠️ Bazı tablolar oluşturulamadı")
    
    return success

def write_dataframe_to_postgresql(df, table_type, mode="append"):
    """
    DataFrame'i PostgreSQL'e yaz
    
    Args:
        df: Spark DataFrame
        table_type (str): TABLE_NAMES içindeki tablo tipi
        mode (str): Yazma modu (append, overwrite)
        
    Returns:
        bool: Başarılı ise True
    """
    table_name = TABLE_NAMES.get(table_type)
    
    if not table_name:
        logger.error(f"❌ '{table_type}' için tablo adı bulunamadı")
        return False
    
    try:
        count = df.count()
        logger.info(f"📝 Veri PostgreSQL'e yazılıyor: {table_name} tablosu ({count} kayıt)")
        
        df.write.jdbc(
            url=JDBC_URL,
            table=table_name,
            mode=mode,
            properties=JDBC_PROPERTIES
        )
        
        logger.info(f"✅ Veri başarıyla PostgreSQL'e yazıldı: {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL'e veri yazma hatası: {e}")
        return False

def save_predictions_to_db(predictions_df, run_id, model_version):
    """
    Tahmin sonuçlarını veritabanına kaydet
    
    Args:
        predictions_df: Tahmin içeren DataFrame
        run_id (str): MLflow run ID
        model_version (str): Model versiyonu
        
    Returns:
        bool: Başarılı ise True
    """
    try:
        logger.info(f"📊 Tahmin sonuçları veritabanına kaydediliyor...")
        
        # Tablo oluştur
        if not create_table_if_not_exists("daily_predictions"):
            return False
        
        # Prediction hatalarını içeren tablo
        if not create_table_if_not_exists("prediction_errors"):
            return False
        
        # Run ID ve model version ekle
        enriched_df = predictions_df \
            .withColumn("run_id", lit(run_id)) \
            .withColumn("model_version", lit(model_version))
        
        # ✅ DÜZELTME: Tarih kolonunu DATE tipine dönüştür
        enriched_df = enriched_df.withColumn("date", col("date").cast(DateType()))
        
        # Günlük tahminleri kaydet
        result1 = write_dataframe_to_postgresql(
            enriched_df.select(
                "run_id", "model_version", "date", 
                col("total_daily_energy").alias("actual_energy"),
                col("prediction").alias("predicted_energy")
            ),
            "daily_predictions"
        )
        
        # Hata metriklerini hesapla ve kaydet
        error_df = enriched_df.withColumn(
            "absolute_error", 
            abs(col("prediction") - col("total_daily_energy"))  # ✅ abs() kullanıldı
        ).withColumn(
            "percentage_error", 
            (col("prediction") - col("total_daily_energy")) / col("total_daily_energy") * 100
        ).withColumn(
            "is_overestimation",
            col("prediction") > col("total_daily_energy")
        ).withColumn(
            "error_category",
            when(abs(col("percentage_error")) < 5, "Düşük")
            .when(abs(col("percentage_error")) < 15, "Orta")  # ✅ when() artık tanımlı
            .otherwise("Yüksek")
        )
        
        result2 = write_dataframe_to_postgresql(
            error_df.select(
                "run_id", "model_version", "date",
                col("total_daily_energy").alias("actual_energy"),
                col("prediction").alias("predicted_energy"),
                col("absolute_error"),
                col("percentage_error"),
                col("is_overestimation"),
                col("error_category")
            ),
            "prediction_errors"
        )
        
        return result1 and result2
        
    except Exception as e:
        logger.error(f"❌ Tahmin kaydetme hatası: {e}")
        return False

def save_model_metrics_to_db(metrics, feature_importance, feature_columns, run_id, model_version):
    """
    Model metriklerini ve özellik önemlerini veritabanına kaydet
    
    Args:
        metrics (dict): Performans metrikleri
        feature_importance (dict): Özellik önemleri
        feature_columns (list): Özellik isimleri
        run_id (str): MLflow run ID
        model_version (str): Model versiyonu
        
    Returns:
        bool: Başarılı ise True
    """
    try:
        logger.info(f"📊 Model metrikleri veritabanına kaydediliyor...")
        spark = get_spark_session()
        
        # Model metrikleri tablosunu oluştur
        if not create_table_if_not_exists("model_metrics"):
            return False
        
        # Özellik önemliliği tablosunu oluştur
        if not create_table_if_not_exists("feature_importance"):
            return False
        
        # Metrikleri veritabanına kaydet
        metrics_data = [{
            "run_id": run_id,
            "model_version": model_version,
            "train_r2": float(metrics.get("train_r2", 0.0)),  # ✅ float() dönüşümü
            "val_r2": float(metrics.get("val_r2", 0.0)),
            "test_r2": float(metrics.get("test_r2", 0.0)),
            "train_rmse": float(metrics.get("train_rmse", 0.0)),
            "val_rmse": float(metrics.get("val_rmse", 0.0)),
            "test_rmse": float(metrics.get("test_rmse", 0.0)),
            "train_mae": float(metrics.get("train_mae", 0.0)),
            "val_mae": float(metrics.get("val_mae", 0.0)),
            "test_mae": float(metrics.get("test_mae", 0.0)),
            "training_time": float(metrics.get("training_time", 0.0)),
            "total_features": len(feature_columns)
        }]
        
        metrics_df = spark.createDataFrame(metrics_data)
        write_dataframe_to_postgresql(metrics_df, "model_metrics")
        
        # Özellik önemlerini veritabanına kaydet
        if feature_importance:
            # Önem skorlarına göre sırala
            sorted_features = sorted(
                feature_importance.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            importance_data = []
            for i, (feature, score) in enumerate(sorted_features, 1):
                importance_data.append({
                    "run_id": run_id,
                    "model_version": model_version,
                    "feature_name": feature,
                    "importance_score": float(score),
                    "importance_rank": i
                })
            
            importance_df = spark.createDataFrame(importance_data)
            write_dataframe_to_postgresql(importance_df, "feature_importance")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Model metrikleri kaydetme hatası: {e}")
        return False

def register_model_in_db(run_id, model_version, hyperparams, description=None):
    """
    Modeli veritabanı kayıt sistemine ekle
    
    Args:
        run_id (str): MLflow run ID
        model_version (str): Model versiyonu
        hyperparams (dict): Model hiperparametreleri
        description (str, optional): Model açıklaması
        
    Returns:
        bool: Başarılı ise True
    """
    try:
        logger.info(f"📝 Model veritabanı kayıt sistemine ekleniyor...")
        spark = get_spark_session()
        
        # Model kayıt tablosunu oluştur
        if not create_table_if_not_exists("model_registry"):
            return False
        
        # Şu anki zamanı al
        import datetime
        current_time = datetime.datetime.now().isoformat()
        
        # ✅ DÜZELTME: DataFrame'de hyperparameters'ı JSON string olarak oluştur
        # ve PostgreSQL'e yazarken cast et
        registry_data = [{
            "run_id": run_id,
            "model_version": model_version,
            "model_name": MLFLOW_CONFIG["model_name"],
            "training_date": current_time,
            "description": description or f"Sistem enerji tahmini modeli v{model_version}",
            "hyperparameters": json.dumps(hyperparams),  # JSON string olarak hazır
            "mlflow_uri": f"{MLFLOW_CONFIG['tracking_uri']}/experiments/{run_id}",
            "local_path": f"{MLFLOW_CONFIG['local_model_path']}/energy_forecaster_v{model_version}",
            "is_production": False
        }]
        
        registry_df = spark.createDataFrame(registry_data)
        

        registry_df = registry_df.withColumn(
            "hyperparameters", 
            expr("CAST(hyperparameters AS STRING)").cast("string")
        )
        
        # ✅ ALTERNATIF: Özel JDBC write options kullan
        table_name = TABLE_NAMES.get("model_registry")
        
        # Manuel JDBC yazma ile casting
        try:
            registry_df.write \
                .mode("append") \
                .option("driver", JDBC_PROPERTIES["driver"]) \
                .option("user", JDBC_PROPERTIES["user"]) \
                .option("password", JDBC_PROPERTIES["password"]) \
                .option("stringtype", "unspecified") \
                .jdbc(JDBC_URL, table_name)
            
            logger.info(f"✅ Model veritabanı kayıt sistemine eklendi")
            return True
            
        except Exception as jdbc_error:
            logger.warning(f"⚠️ JDBC yazma hatası, psycopg2 ile deneniyor: {jdbc_error}")
            

            
            conn = psycopg2.connect(
                host=POSTGRES_CONFIG["host"],
                port=POSTGRES_CONFIG["port"],
                database=POSTGRES_CONFIG["database"],
                user=POSTGRES_CONFIG["user"],
                password=POSTGRES_CONFIG["password"]
            )
            
            cursor = conn.cursor()
            
            # JSONB ile INSERT
            insert_sql = """
            INSERT INTO ml_model_registry 
            (run_id, model_version, model_name, training_date, description, 
             hyperparameters, mlflow_uri, local_path, is_production)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                run_id,
                model_version,
                MLFLOW_CONFIG["model_name"],
                current_time,
                description or f"Sistem enerji tahmini modeli v{model_version}",
                Json(hyperparams),  # psycopg2 Json wrapper kullan
                f"{MLFLOW_CONFIG['tracking_uri']}/experiments/{run_id}",
                f"{MLFLOW_CONFIG['local_model_path']}/energy_forecaster_v{model_version}",
                False
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Model veritabanı kayıt sistemine eklendi (psycopg2 ile)")
            return True
        
    except Exception as e:
        logger.error(f"❌ Model kaydetme hatası: {e}")
        return False
def get_table_count(table_type):
    """
    Tablodaki kayıt sayısını getir
    
    Args:
        table_type (str): TABLE_NAMES içindeki tablo tipi
        
    Returns:
        int: Kayıt sayısı
    """
    table_name = TABLE_NAMES.get(table_type)
    
    if not table_name:
        logger.error(f"❌ '{table_type}' için tablo adı bulunamadı")
        return 0
    
    try:
        spark = get_spark_session()
        count_df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT COUNT(*) as count FROM {table_name}) AS count_query",
            properties=JDBC_PROPERTIES
        )
        
        return count_df.collect()[0]["count"]
        
    except Exception as e:
        logger.error(f"❌ Tablo sayım hatası: {e}")
        return 0

def load_data_from_postgresql(table_type, condition=None):
    """
    PostgreSQL'den veri yükle
    
    Args:
        table_type (str): TABLE_NAMES içindeki tablo tipi
        condition (str, optional): WHERE koşulu
        
    Returns:
        DataFrame: Yüklenen veri
    """
    table_name = TABLE_NAMES.get(table_type)
    
    if not table_name:
        logger.error(f"❌ '{table_type}' için tablo adı bulunamadı")
        return None
    
    try:
        spark = get_spark_session()
        
        if condition:
            query = f"(SELECT * FROM {table_name} WHERE {condition}) AS query"
        else:
            query = f"(SELECT * FROM {table_name}) AS query"
        
        df = spark.read.jdbc(
            url=JDBC_URL,
            table=query,
            properties=JDBC_PROPERTIES
        )
        
        count = df.count()
        logger.info(f"📊 {table_name} tablosundan {count} kayıt yüklendi")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Veri yükleme hatası: {e}")
        return None

def cleanup_connections():
    """Tüm bağlantıları temizle"""
    stop_spark_session()
    logger.info("🧹 Tüm bağlantılar temizlendi")