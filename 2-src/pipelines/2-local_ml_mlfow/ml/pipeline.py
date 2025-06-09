#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Makine Öğrenimi Pipeline Modülü

Bu modül, ML pipeline oluşturma, eğitme ve değerlendirme işlevlerini sağlar.
"""

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from utils.logger import get_logger
from utils.config import MODEL_CONFIG
from utils.mlflow_utils import save_model_predictions_to_db

# Logger oluştur
logger = get_logger(__name__)

def create_ml_pipeline(feature_columns):
    """
    Özellik ölçeklendirme ve GBT regresyonu ile ML pipeline oluştur
    
    Args:
        feature_columns: Özellik kolonu isimleri
        
    Returns:
        Pipeline: Oluşturulan ML pipeline
    """
    logger.info("🤖 ML PIPELINE OLUŞTURULUYOR...")
    
    # Hiperparametreleri yapılandırmadan al
    max_depth = MODEL_CONFIG.get("max_depth", 6)
    max_bins = MODEL_CONFIG.get("max_bins", 32)
    max_iter = MODEL_CONFIG.get("max_iter", 100)
    step_size = MODEL_CONFIG.get("step_size", 0.1)
    subsampling_rate = MODEL_CONFIG.get("subsampling_rate", 0.8)
    feature_subset_strategy = MODEL_CONFIG.get("feature_subset_strategy", "sqrt")
    seed = MODEL_CONFIG.get("seed", 42)
    
    # Vektör dönüştürücü
    logger.info(f"🔢 Özellik vektörleri oluşturuluyor ({len(feature_columns)} özellik)")
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="raw_features"
    )
    
    # Özellik ölçeklendirici
    logger.info("📏 Özellik ölçeklendirme tanımlanıyor (StandardScaler)")
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    # GBT Regresyonu
    logger.info(f"🌳 GBT Regresyonu tanımlanıyor (max_depth={max_depth}, max_iter={max_iter})")
    gbt = GBTRegressor(
        featuresCol="scaled_features",
        labelCol="total_daily_energy",
        predictionCol="prediction",
        maxDepth=max_depth,
        maxBins=max_bins,
        maxIter=max_iter,
        stepSize=step_size,
        subsamplingRate=subsampling_rate,
        featureSubsetStrategy=feature_subset_strategy,
        seed=seed
    )
    
    # Pipeline oluştur
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    logger.info(f"✅ Pipeline oluşturuldu ({len(feature_columns)} özellik)")
    logger.info(f"🎯 Hedef: total_daily_energy")
    logger.info(f"🌳 Algoritma: Gradient Boosted Trees")
    logger.info(f"⚙️ Hiperparametreler: max_depth={max_depth}, max_iter={max_iter}, step_size={step_size}")
    
    return pipeline

def train_and_evaluate_model(pipeline, train_df, val_df, test_df):
    """
    Modeli eğit ve performansını değerlendir
    
    Args:
        pipeline: ML pipeline
        train_df: Eğitim DataFrame'i
        val_df: Doğrulama DataFrame'i
        test_df: Test DataFrame'i
        
    Returns:
        tuple: (model, metrics, test_predictions)
    """
    logger.info("🚀 MODEL EĞİTİLİYOR...")
    train_start = time.time()
    
    # Pipeline'ı eğit
    model = pipeline.fit(train_df)
    
    training_time = time.time() - train_start
    logger.info(f"✅ Eğitim {training_time:.1f}s sürede tamamlandı")
    
    # Tahminler yap
    logger.info("📊 MODEL DEĞERLENDİRİLİYOR...")
    
    train_predictions = model.transform(train_df)
    val_predictions = model.transform(val_df)
    test_predictions = model.transform(test_df)
    
    # Değerlendirici
    evaluator = RegressionEvaluator(
        labelCol="total_daily_energy",
        predictionCol="prediction"
    )
    
    # Metrikleri hesapla
    def calculate_metrics(predictions_df, dataset_name):
        r2 = evaluator.evaluate(predictions_df, {evaluator.metricName: "r2"})
        rmse = evaluator.evaluate(predictions_df, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions_df, {evaluator.metricName: "mae"})
        
        # Gerçek ve tahmin değerlerini topla
        pd_df = predictions_df.select("date", "total_daily_energy", "prediction").toPandas()
        
        # Ek metrikler hesapla
        mape = np.mean(np.abs((pd_df["total_daily_energy"] - pd_df["prediction"]) / pd_df["total_daily_energy"])) * 100
        
        # Maksimum hata
        max_error = np.max(np.abs(pd_df["total_daily_energy"] - pd_df["prediction"]))
        max_error_date = pd_df.loc[np.abs(pd_df["total_daily_energy"] - pd_df["prediction"]).idxmax(), "date"]
        
        return {
            f"{dataset_name}_r2": r2,
            f"{dataset_name}_rmse": rmse,
            f"{dataset_name}_mae": mae,
            f"{dataset_name}_mape": mape,
            f"{dataset_name}_max_error": max_error,
            f"{dataset_name}_max_error_date": max_error_date
        }
    
    # Tüm setler için metrikleri al
    train_metrics = calculate_metrics(train_predictions, "train")
    val_metrics = calculate_metrics(val_predictions, "val")
    test_metrics = calculate_metrics(test_predictions, "test")
    
    # Tüm metrikleri birleştir
    all_metrics = {**train_metrics, **val_metrics, **test_metrics, "training_time": training_time}
    
    # Sonuçları göster
    logger.info(f"🎯 MODEL PERFORMANSI:")
    logger.info(f"   📚 Eğitim  R²: {train_metrics['train_r2']:.4f} | RMSE: {train_metrics['train_rmse']:.2f} | MAE: {train_metrics['train_mae']:.2f} | MAPE: {train_metrics['train_mape']:.2f}%")
    logger.info(f"   🔍 Doğrulama R²: {val_metrics['val_r2']:.4f} | RMSE: {val_metrics['val_rmse']:.2f} | MAE: {val_metrics['val_mae']:.2f} | MAPE: {val_metrics['val_mape']:.2f}%")
    logger.info(f"   🧪 Test   R²: {test_metrics['test_r2']:.4f} | RMSE: {test_metrics['test_rmse']:.2f} | MAE: {test_metrics['test_mae']:.2f} | MAPE: {test_metrics['test_mape']:.2f}%")
    
    # Aşırı öğrenme analizi
    train_test_gap = train_metrics['train_r2'] - test_metrics['test_r2']
    logger.info(f"   🔬 Aşırı Öğrenme Boşluğu: {train_test_gap:.4f}")
    
    if train_test_gap > 0.1:
        logger.warning("   ⚠️  Model aşırı öğrenme gösteriyor olabilir")
    else:
        logger.info("   ✅ İyi genelleme")
    
    # Tahmin görselleştirmesi oluştur
    try:
        create_prediction_plot(test_predictions, "test_predictions.png")
        logger.info("📊 Test tahminleri grafiği oluşturuldu: test_predictions.png")
    except Exception as e:
        logger.warning(f"⚠️ Tahmin grafiği oluşturulamadı: {e}")
    
    # Tahminleri veritabanına kaydetmeyi dene
    try:
        # MLflow run_id al (aktif çalışma varsa)
        import mlflow
        run_id = mlflow.active_run().info.run_id if mlflow.active_run() else None
        
        if run_id:
            # Veritabanına kaydet
            save_model_predictions_to_db(test_predictions, run_id)
        else:
            logger.warning("⚠️ MLflow çalışması bulunamadı, tahminler veritabanına kaydedilemiyor")
    except Exception as e:
        logger.warning(f"⚠️ Tahminlerin veritabanına kaydedilmesi başarısız: {e}")
    
    return model, all_metrics, test_predictions

def create_prediction_plot(predictions_df, filename=None):
    """
    Tahmin-gerçek değer karşılaştırma grafiği oluştur
    
    Args:
        predictions_df: Tahmin içeren DataFrame
        filename: Kaydedilecek dosya adı (None ise kaydetmez)
    """
    # Pandas DataFrame'ine dönüştür
    pdf = predictions_df.select("date", "total_daily_energy", "prediction").toPandas()
    
    # Tarihe göre sırala
    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf = pdf.sort_values("date")
    
    # Grafiği oluştur
    plt.figure(figsize=(12, 6))
    
    # Gerçek değerler
    plt.plot(pdf["date"], pdf["total_daily_energy"], 'b-', label='Gerçek Değer', linewidth=2)
    
    # Tahminler
    plt.plot(pdf["date"], pdf["prediction"], 'r--', label='Tahmin', linewidth=2)
    
    # Hata alanı
    plt.fill_between(pdf["date"], 
                    pdf["total_daily_energy"], 
                    pdf["prediction"], 
                    color='gray', alpha=0.3, label='Hata')
    
    # Grafik biçimlendirme
    plt.title('Toplam Günlük Enerji: Gerçek vs Tahmin')
    plt.xlabel('Tarih')
    plt.ylabel('Enerji')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Dosyaya kaydet
    if filename:
        plt.savefig(filename, dpi=300, bbox_inches='tight')
    
    plt.close()