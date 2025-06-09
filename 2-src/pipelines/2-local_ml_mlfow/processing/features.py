#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Özellik Mühendisliği Modülü

Bu modül, zaman serisi verisi için ileri düzey özellikler oluşturur ve
ML modellemesi için veri setini hazırlar.
"""

import time
from pyspark.sql.functions import (
    col, lag, sin, cos, when, avg as spark_avg, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from utils.logger import get_logger
from utils.config import PREPROCESSING_CONFIG, MODEL_CONFIG
from utils.spark_utils import cache_dataframe, show_dataframe_info

# Logger oluştur
logger = get_logger(__name__)

def create_advanced_features(daily_df):
    """
    Gecikmeli özellikler, hareketli ortalamalar ve zaman özellikleri oluştur
    
    Args:
        daily_df: Günlük toplam enerji DataFrame'i
        
    Returns:
        tuple: (feature_df, feature_columns, processing_time)
    """
    logger.info("🧠 GELİŞMİŞ ÖZELLİKLER OLUŞTURULUYOR...")
    start_time = time.time()
    
    # Zaman penceresi tanımla (tarih sırasına göre)
    window_spec = Window.orderBy("date")
    
    # 1. Gecikmeli özellikler (önceki günler)
    logger.info("⏱️  Gecikmeli özellikler oluşturuluyor...")
    daily_df = daily_df \
        .withColumn("energy_lag_1", lag("total_daily_energy", 1).over(window_spec)) \
        .withColumn("energy_lag_2", lag("total_daily_energy", 2).over(window_spec)) \
        .withColumn("energy_lag_3", lag("total_daily_energy", 3).over(window_spec)) \
        .withColumn("energy_lag_7", lag("total_daily_energy", 7).over(window_spec))
    
    # 2. Hareketli ortalamalar (pencere ağırlıkları)
    logger.info("📊 Hareketli ortalama özellikleri oluşturuluyor...")
    rolling_window_3 = Window.orderBy("date").rowsBetween(-2, 0)  # Son 3 gün
    rolling_window_7 = Window.orderBy("date").rowsBetween(-6, 0)  # Son 7 gün
    rolling_window_30 = Window.orderBy("date").rowsBetween(-29, 0)  # Son 30 gün
    
    daily_df = daily_df \
        .withColumn("rolling_avg_3d", spark_avg("total_daily_energy").over(rolling_window_3)) \
        .withColumn("rolling_avg_7d", spark_avg("total_daily_energy").over(rolling_window_7)) \
        .withColumn("rolling_avg_30d", spark_avg("total_daily_energy").over(rolling_window_30)) \
        .withColumn("rolling_max_7d", spark_max("total_daily_energy").over(rolling_window_7)) \
        .withColumn("rolling_min_7d", spark_min("total_daily_energy").over(rolling_window_7))
    
    # 3. Döngüsel zaman özellikleri (sinüs/kosinüs kodlama)
    logger.info("🔄 Döngüsel zaman özellikleri oluşturuluyor...")
    daily_df = daily_df \
        .withColumn("month_sin", sin(col("month") * 2 * 3.14159 / 12)) \
        .withColumn("month_cos", cos(col("month") * 2 * 3.14159 / 12)) \
        .withColumn("dayofweek_sin", sin(col("dayofweek") * 2 * 3.14159 / 7)) \
        .withColumn("dayofweek_cos", cos(col("dayofweek") * 2 * 3.14159 / 7))
    
    # 4. Türetilmiş özellikler (kategorik ve mevsimsel)
    logger.info("🌞 Mevsimsel ve kategorik özellikler oluşturuluyor...")
    daily_df = daily_df \
        .withColumn("is_weekend", when(col("dayofweek").isin([1, 7]), 1).otherwise(0)) \
        .withColumn("is_summer", when(col("month").isin([12, 1, 2]), 1).otherwise(0)) \
        .withColumn("is_winter", when(col("month").isin([6, 7, 8]), 1).otherwise(0)) \
        .withColumn("peak_to_avg_ratio", col("peak_daily_load") / col("avg_daily_load"))
    
    # 5. Eğilim (trend) özellikleri
    logger.info("📈 Eğilim özellikleri oluşturuluyor...")
    daily_df = daily_df \
        .withColumn("energy_trend_3d", col("total_daily_energy") - col("rolling_avg_3d")) \
        .withColumn("energy_trend_7d", col("total_daily_energy") - col("rolling_avg_7d"))
    
    # Önbelleğe al
    feature_df = cache_dataframe(daily_df, "Gelişmiş Özellikler DataFrame")
    
    # İşleme süresini hesapla
    processing_time = time.time() - start_time
    
    # Özellik listesini oluştur (hedef değişken hariç)
    feature_columns = [c for c in feature_df.columns if c not in ['date', 'timestamp', 'total_daily_energy']]
    
    # Özellik özetleri
    logger.info(f"✅ Gelişmiş özellikler {processing_time:.1f}s sürede oluşturuldu")
    logger.info(f"📊 Toplam özellik sayısı: {len(feature_columns)}")
    
    # Özellik kategorilerine göre grupla ve logla
    logger.info("🧩 ÖZELLİK KATEGORİLERİ:")
    
    # Gecikmeli özellikler
    lag_features = [f for f in feature_columns if 'lag' in f]
    logger.info(f"   ⏱️  Gecikmeli özellikler ({len(lag_features)}): {', '.join(lag_features)}")
    
    # Hareketli ortalama özellikleri
    rolling_features = [f for f in feature_columns if 'rolling' in f]
    logger.info(f"   📊 Hareketli ortalama özellikleri ({len(rolling_features)}): {', '.join(rolling_features)}")
    
    # Zaman özellikleri
    time_features = [f for f in feature_columns if any(x in f for x in ['sin', 'cos', 'month', 'day'])]
    logger.info(f"   🔄 Zaman özellikleri ({len(time_features)}): {', '.join(time_features)}")
    
    # Kategorik özellikler
    categorical_features = [f for f in feature_columns if f.startswith('is_')]
    logger.info(f"   🏷️  Kategorik özellikler ({len(categorical_features)}): {', '.join(categorical_features)}")
    
    # Eğilim özellikleri
    trend_features = [f for f in feature_columns if 'trend' in f]
    logger.info(f"   📈 Eğilim özellikleri ({len(trend_features)}): {', '.join(trend_features)}")
    
    # Diğer özellikler
    other_features = [f for f in feature_columns if f not in lag_features + rolling_features + 
                      time_features + categorical_features + trend_features]
    if other_features:
        logger.info(f"   🔧 Diğer özellikler ({len(other_features)}): {', '.join(other_features)}")
    
    return feature_df, feature_columns, processing_time

def prepare_ml_dataset(daily_df, feature_columns, test_days=None):
    """
    Eğitim/doğrulama/test bölümleri ile ML veri seti hazırla
    
    Args:
        daily_df: Özellik eklenmiş DataFrame
        feature_columns: Özellik kolonu isimleri
        test_days: Test için ayrılacak gün sayısı (None ise config'den alınır)
        
    Returns:
        tuple: (train_df, val_df, test_df, train_count, val_count, test_count)
    """
    # Test gün sayısını config'den al (belirtilmemişse)
    if test_days is None:
        test_days = MODEL_CONFIG.get("test_days", 60)
    
    logger.info(f"🎯 ML VERİ SETİ HAZIRLANIYOR...")
    logger.info(f"📝 Test periyodu: Son {test_days} gün")
    
    # Null değerleri temizle (gecikmeli özelliklerden kaynaklı)
    clean_df = daily_df.dropna()
    
    total_rows = clean_df.count()
    logger.info(f"📊 Temiz veri seti: {total_rows} gün")
    
    # Veri yeterli mi kontrol et
    if total_rows < test_days + 30:
        logger.warning(f"⚠️  UYARI: Yeterli veri yok! Sadece {total_rows} gün mevcut")
        test_days = max(10, total_rows // 4)
        logger.warning(f"🔄 Test günleri şu şekilde ayarlandı: {test_days}")
    
    # Sıralama için satır numarası ekle
    window_spec = Window.orderBy("date")
    indexed_df = clean_df.withColumn("row_num", row_number().over(window_spec))
    
    # Veri setini böl
    train_val_ratio = PREPROCESSING_CONFIG.get("train_val_test_split", [0.7, 0.15, 0.15])
    val_size = 15  # Doğrulama için 15 gün rezerve et
    
    train_size = total_rows - test_days - val_size
    
    train_df = indexed_df.filter(col("row_num") <= train_size)
    val_df = indexed_df.filter((col("row_num") > train_size) & (col("row_num") <= train_size + val_size))
    test_df = indexed_df.filter(col("row_num") > train_size + val_size)
    
    # Set büyüklüklerini hesapla
    train_count = train_df.count()
    val_count = val_df.count()
    test_count = test_df.count()
    
    # Veri bölünmesi hakkında bilgi
    logger.info(f"✅ VERİ BÖLÜNMESİ:")
    logger.info(f"   🏋️  Eğitim: {train_count} gün ({train_count/total_rows*100:.1f}%)")
    logger.info(f"   🔍 Doğrulama: {val_count} gün ({val_count/total_rows*100:.1f}%)")
    logger.info(f"   🧪 Test: {test_count} gün ({test_count/total_rows*100:.1f}%)")
    
    # Tarih aralıklarını göster - ✅ DÜZELTME: spark_min ve spark_max kullan
    train_dates = train_df.select(spark_min("date").alias("start"), spark_max("date").alias("end")).collect()[0]
    val_dates = val_df.select(spark_min("date").alias("start"), spark_max("date").alias("end")).collect()[0]
    test_dates = test_df.select(spark_min("date").alias("start"), spark_max("date").alias("end")).collect()[0]
    
    logger.info(f"   📅 Eğitim tarihleri: {train_dates['start']} - {train_dates['end']}")
    logger.info(f"   📅 Doğrulama tarihleri: {val_dates['start']} - {val_dates['end']}")
    logger.info(f"   📅 Test tarihleri: {test_dates['start']} - {test_dates['end']}")
    
    # Özellik istatistikleri
    logger.info(f"   🧩 Toplam özellik sayısı: {len(feature_columns)}")
    
    return train_df, val_df, test_df, train_count, val_count, test_count