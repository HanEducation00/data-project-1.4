#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MLflow Yardımcı Fonksiyonları

Bu modül MLflow'a sonuçları kaydetmek ve PostgreSQL veritabanı entegrasyonu sağlamak için fonksiyonlar içerir.
"""

import mlflow
import mlflow.spark
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

from utils.logger import get_logger
from utils.config import MLFLOW_CONFIG, MODEL_CONFIG
from .connections import (
    setup_mlflow, 
    save_model_metrics_to_db, 
    register_model_in_db,
    save_predictions_to_db
)

# Logger oluştur
logger = get_logger(__name__)

def log_to_mlflow(metrics, feature_columns, importance_dict, hyperparams, model=None):
    """
    Sonuçları MLflow'a ve PostgreSQL'e kaydet
    
    Args:
        metrics (dict): Performans metrikleri
        feature_columns (list): Özellik isimleri
        importance_dict (dict): Özellik önemleri
        hyperparams (dict): Model hiperparametreleri
        model: Eğitilmiş model (opsiyonel)
        
    Returns:
        bool: Başarılı ise True
    """
    logger.info("📝 MLFLOW'A SONUÇLARI KAYDETME...")
    
    # MLflow bağlantısını kontrol et
    _, experiment_name, _ = setup_mlflow()
    
    try:
        # Run ID ve model versiyonu oluştur
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = mlflow.active_run().info.run_id if mlflow.active_run() else None
        model_version = f"{timestamp}"
        
        # Hiperparametreleri kaydet
        logger.info("⚙️ Hiperparametreleri kaydediliyor...")
        for param, value in hyperparams.items():
            # ✅ DÜZELTME: Date objelerini string'e dönüştür
            if hasattr(value, 'strftime'):  # datetime objesi ise
                value = value.strftime('%Y-%m-%d')
            mlflow.log_param(param, value)
        
        # Metrikleri kaydet
        logger.info("📏 Performans metriklerini kaydediliyor...")
        for metric, value in metrics.items():
            # ✅ DÜZELTME: Sadece sayısal değerleri metrik olarak kaydet
            try:
                if isinstance(value, (int, float)):
                    mlflow.log_metric(metric, float(value))
                elif isinstance(value, str):
                    # String değerleri parametre olarak kaydet
                    mlflow.log_param(f"info_{metric}", value)
                else:
                    # Diğer tipleri string'e dönüştürüp parametre olarak kaydet
                    mlflow.log_param(f"info_{metric}", str(value))
            except Exception as e:
                logger.warning(f"⚠️ {metric} metriği kaydedilemedi: {e}")
        
        # Özellik bilgilerini kaydet
        logger.info("🧩 Özellik bilgilerini kaydediliyor...")
        mlflow.log_param("total_features", len(feature_columns))
        mlflow.log_param("feature_list", ", ".join(feature_columns[:10]))
        
        # Özellik önemlerini kaydet
        for feature, importance in importance_dict.items():
            try:
                mlflow.log_metric(f"feature_importance_{feature}", float(importance))
            except Exception as e:
                logger.warning(f"⚠️ {feature} özellik önemliliği kaydedilemedi: {e}")
        
        # Model kaydetme
        if model is not None:
            logger.info("💾 Modeli MLflow'a kaydediliyor...")
            try:
                # MLflow Model Registry'e kaydet
                mlflow.spark.log_model(
                    spark_model=model,
                    artifact_path=MLFLOW_CONFIG["artifact_path"],
                    registered_model_name=MLFLOW_CONFIG["model_name"]
                )
                logger.info(f"✅ Model MLflow Model Registry'e kaydedildi: {MLFLOW_CONFIG['model_name']}")
                
                # Yerel yedek oluştur
                try:
                    import os
                    model_dir = MLFLOW_CONFIG["local_model_path"]
                    os.makedirs(model_dir, exist_ok=True)
                    
                    model_path = f"{model_dir}/energy_forecaster_pipeline"
                    model.write().overwrite().save(model_path)
                    logger.info(f"✅ Model yerel olarak kaydedildi: {model_path}")
                    
                    # Yerel yolu MLflow'a ekle
                    mlflow.log_param("local_model_path", model_path)
                except Exception as e:
                    logger.warning(f"⚠️ Yerel model yedekleme hatası: {e}")
                
                # Model kayıt durumunu logla
                mlflow.log_param("model_save_status", "SUCCESS")
                
            except Exception as e:
                logger.error(f"❌ Model kaydetme hatası: {e}")
                mlflow.log_param("model_save_status", "FAILED")
                mlflow.log_param("model_save_error", str(e))
        
        # PostgreSQL'e de kaydet
        try:
            # Model metriklerini ve özellik önemlerini veritabanına kaydet
            save_model_metrics_to_db(metrics, importance_dict, feature_columns, run_id, model_version)
            
            # Model kayıt sistemine ekle
            register_model_in_db(run_id, model_version, hyperparams)
            
            logger.info("✅ Sonuçlar PostgreSQL'e kaydedildi")
        except Exception as e:
            logger.warning(f"⚠️ PostgreSQL kaydetme hatası: {e}")
        
        logger.info("✅ MLflow loglama tamamlandı")
        return True
        
    except Exception as e:
        logger.error(f"❌ MLflow loglama hatası: {e}")
        return False

def save_model_predictions_to_db(predictions_df, run_id):
    """
    Model tahminlerini PostgreSQL'e kaydet
    
    Args:
        predictions_df: Tahmin sonuçlarını içeren DataFrame
        run_id: MLflow run ID
        
    Returns:
        bool: Başarılı ise True
    """
    logger.info("💾 Model tahminlerini PostgreSQL'e kaydediliyor...")
    
    try:
        # Model versiyonu oluştur
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_version = f"{timestamp}"
        
        # Tahminleri veritabanına kaydet
        result = save_predictions_to_db(predictions_df, run_id, model_version)
        
        if result:
            logger.info("✅ Tahminler PostgreSQL'e başarıyla kaydedildi")
        else:
            logger.warning("⚠️ Tahminlerin PostgreSQL'e kaydedilmesinde sorun oluştu")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Tahminleri PostgreSQL'e kaydederken hata: {e}")
        return False

def create_feature_importance_plot(importance_dict, model=None):
    """
    Özellik önemliliği grafiği oluştur ve MLflow'a kaydet
    
    Args:
        importance_dict (dict): Özellik önemleri
        model: Model (özellik önemliliği alınabilir modellerden)
    """
    # Model verilmişse özellik önemliliğini al
    if model is not None and not importance_dict:
        try:
            # GBT modelini al (pipeline'ın son aşaması)
            gbt_model = model.stages[-1]
            importance_scores = gbt_model.featureImportances.toArray()
            
            # Özellik isimleri ve skorları eşleştir
            importance_dict = {}
            for i, score in enumerate(importance_scores):
                feature_name = f"feature_{i}"  # Gerçek isimleri bilmiyoruz
                importance_dict[feature_name] = float(score)
        except Exception as e:
            logger.warning(f"⚠️ Modelden özellik önemliliği çıkarılamadı: {e}")
    
    if not importance_dict:
        logger.warning("⚠️ Özellik önemliliği verisi yok, grafik oluşturulamadı")
        return
    
    try:
        # En önemli 10 özelliği al
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        top_features = sorted_features[:min(10, len(sorted_features))]
        
        # Veriyi hazırla
        feature_names = [item[0] for item in top_features]
        importance_values = [item[1] for item in top_features]
        
        # Grafiği oluştur
        plt.figure(figsize=(10, 6))
        plt.barh(range(len(top_features)), importance_values, align='center')
        plt.yticks(range(len(top_features)), feature_names)
        plt.xlabel('Önem Skoru')
        plt.title('Özellik Önemliliği')
        
        # Dosyaya kaydet
        filename = f"feature_importance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(filename, bbox_inches='tight')
        plt.close()
        
        # MLflow'a kaydet
        mlflow.log_artifact(filename, "visualizations")
        logger.info(f"✅ Özellik önemliliği grafiği oluşturuldu ve MLflow'a kaydedildi: {filename}")
        
    except Exception as e:
        logger.error(f"❌ Özellik önemliliği grafiği oluşturma hatası: {e}")