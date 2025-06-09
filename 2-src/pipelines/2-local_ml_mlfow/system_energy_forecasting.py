#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SİSTEM TOPLAM ENERJİ TAHMİN PIPELINE'I

Akıllı sayaç verilerinden günlük toplam sistem enerji tüketimini tahmin eder.
"""

import sys
import os
import time
import mlflow
from datetime import datetime

# Root dizini Python yoluna ekle
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Modülleri içe aktar
from utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from utils.config import MODEL_CONFIG, MLFLOW_CONFIG
from utils.connections import (
    setup_mlflow, get_spark_session, test_postgresql_connection,
    create_all_tables, cleanup_connections
)
from utils.spark_utils import create_spark_session
from utils.mlflow_utils import log_to_mlflow, save_model_predictions_to_db

from data.loader import load_smart_meter_data
from processing.aggregation import create_system_level_aggregation, create_daily_total_energy
from processing.features import create_advanced_features, prepare_ml_dataset
from ml.pipeline import create_ml_pipeline, train_and_evaluate_model
from ml.evaluation import get_feature_importance, analyze_feature_groups

# Logger oluştur
logger = get_logger(__name__)

def main():
    """Ana pipeline yürütme fonksiyonu"""
    # Pipeline başlangıcını logla
    log_pipeline_start()
    logger.info("🌟 SİSTEM TOPLAM ENERJİ TAHMİN PIPELINE'I")
    logger.info("="*80)
    
    spark = None
    
    try:
        # 1. PostgreSQL bağlantısını kontrol et
        logger.info("🔌 PostgreSQL bağlantısı kontrol ediliyor...")
        if not test_postgresql_connection():
            logger.warning("⚠️ PostgreSQL bağlantısı kurulamadı! Veriler sadece MLflow'a kaydedilecek.")
        else:
            # Tabloları oluştur
            logger.info("📊 PostgreSQL tabloları oluşturuluyor...")
            create_all_tables()
        
        # 2. MLflow ayarla
        tracking_uri, experiment_name, experiment_id = setup_mlflow()
        
        # 3. MLflow çalışması başlat
        run_name = f"System_Energy_FullYear_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"🚀 MLflow çalışması başlatılıyor: {run_name}")
        
        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            logger.info(f"🆔 MLflow Run ID: {run_id}")
            pipeline_start = time.time()
            
            try:
                # 4. Spark session oluştur
                spark = create_spark_session()
                
                # 5. Veri yükleme
                logger.info("📂 VERİ YÜKLEME AŞAMASI...")
                df, record_count = load_smart_meter_data(spark)
                
                # 6. Sistem seviyesi toplama
                logger.info("📊 SİSTEM SEVİYESİ TOPLAMA AŞAMASI...")
                system_df, system_time = create_system_level_aggregation(df)
                
                # 7. Günlük toplam enerji hesaplama
                logger.info("📈 GÜNLÜK TOPLAM ENERJİ AŞAMASI...")
                daily_df, day_count, daily_time = create_daily_total_energy(system_df)
                
                # 8. Gelişmiş özellikler oluşturma
                logger.info("🧠 GELİŞMİŞ ÖZELLİKLER AŞAMASI...")
                feature_df, feature_columns, feature_time = create_advanced_features(daily_df)
                
                # 9. ML veri seti hazırlama
                logger.info("🔢 ML VERİ SETİ HAZIRLAMA AŞAMASI...")
                test_days = MODEL_CONFIG.get("test_days", 60)
                train_df, val_df, test_df, train_count, val_count, test_count = prepare_ml_dataset(
                    feature_df, feature_columns, test_days=test_days
                )
                
                # 10. ML pipeline oluşturma
                logger.info("🤖 ML PIPELINE OLUŞTURMA AŞAMASI...")
                ml_pipeline = create_ml_pipeline(feature_columns)
                
                # 11. Hiperparametreleri config'den al
                hyperparams = {
                    "algorithm": MODEL_CONFIG.get("algorithm", "GBTRegressor"),
                    "maxDepth": MODEL_CONFIG.get("max_depth", 6),
                    "maxBins": MODEL_CONFIG.get("max_bins", 32),
                    "maxIter": MODEL_CONFIG.get("max_iter", 100),
                    "stepSize": MODEL_CONFIG.get("step_size", 0.1),
                    "subsamplingRate": MODEL_CONFIG.get("subsampling_rate", 0.8),
                    "featureSubsetStrategy": MODEL_CONFIG.get("feature_subset_strategy", "sqrt"),
                    "seed": MODEL_CONFIG.get("seed", 42),
                    "test_days": test_days,
                    "total_features": len(feature_columns),
                    "spark_version": spark.version
                }
                
                # 12. Model eğitimi ve değerlendirme
                logger.info("🚀 MODEL EĞİTİMİ VE DEĞERLENDİRME AŞAMASI...")
                model, metrics, test_predictions = train_and_evaluate_model(
                    ml_pipeline, train_df, val_df, test_df
                )
                
                # 13. Özellik önemliliği analizi
                logger.info("📊 ÖZELLİK ÖNEMLİLİĞİ ANALİZ AŞAMASI...")
                importance_dict = get_feature_importance(model, feature_columns)
                
                # 14. Özellik grupları analizi
                logger.info("🔍 ÖZELLİK GRUPLARI ANALİZ AŞAMASI...")
                group_importance = analyze_feature_groups(importance_dict, feature_columns)
                
                # 15. MLflow'a sonuçları kaydet
                logger.info("💾 MLFLOW'A SONUÇLARI KAYDETME AŞAMASI...")
                log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model)
                
                # 16. Tahminleri veritabanına kaydet
                logger.info("💾 TAHMİNLERİ VERİTABANINA KAYDETME AŞAMASI...")
                save_model_predictions_to_db(test_predictions, run_id)
                
                # 17. Ek metrikler
                logger.info("📈 EK METRİKLERİ KAYDETME AŞAMASI...")
                mlflow.log_metric("total_raw_records", record_count)
                mlflow.log_metric("total_days", day_count)
                mlflow.log_metric("train_days", train_count)
                mlflow.log_metric("val_days", val_count)
                mlflow.log_metric("test_days", test_count)
                mlflow.log_metric("system_aggregation_time", system_time)
                mlflow.log_metric("daily_aggregation_time", daily_time)
                mlflow.log_metric("feature_engineering_time", feature_time)
                
                # 18. Final özeti
                total_pipeline_time = time.time() - pipeline_start
                mlflow.log_metric("total_pipeline_time", total_pipeline_time)
                
                # Başarı durumunu logla
                mlflow.log_param("pipeline_status", "SUCCESS")
                
                logger.info(f"✅ PIPELINE BAŞARIYLA TAMAMLANDI! (Toplam süre: {total_pipeline_time:.1f}s)")
                logger.info(f"🔗 MLflow UI: {tracking_uri}")
                logger.info(f"🧪 Deney: {experiment_name}")
                logger.info(f"🆔 Run ID: {run_id}")
                
                return True
                
            except Exception as e:
                logger.error(f"❌ PIPELINE HATASI: {e}")
                import traceback
                logger.debug(f"Hata detayları:\n{traceback.format_exc()}")
                
                # Hata logları
                mlflow.log_param("pipeline_status", "FAILED")
                mlflow.log_param("error_message", str(e))
                return False
                
    except Exception as outer_e:
        logger.error(f"❌ DIŞSAL HATA: {outer_e}")
        import traceback
        logger.debug(f"Dışsal hata detayları:\n{traceback.format_exc()}")
        return False
        
    finally:
        # Tüm bağlantıları temizle
        if spark is not None:
            spark.stop()
            logger.info("✅ Spark session kapatıldı")
        
        # Diğer bağlantıları temizle
        cleanup_connections()
        
        # Pipeline sonunu logla
        log_pipeline_end()

if __name__ == "__main__":
    main()