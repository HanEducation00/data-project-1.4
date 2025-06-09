#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Üretim Modülü

Bu modül, gerçekçi akıllı sayaç yük verileri üretir.
"""

import time
import numpy as np
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from utils.logger import get_logger
from utils.config import DATA_CONFIG

# Veri üretim parametreleri (config.py'a eklenecek)
GENERATOR_CONFIG = {
    "start_date": "2016-01-01",
    "end_date": "2016-12-31 23:30:00",
    "time_interval": "30min",
    "customer_count": 150,
    "customer_id_prefix": "AUS_CUSTOMER_",
    "base_load": 35,
    "random_noise_std": 5,
    "min_load": 10,
    "max_load": 90,
    "participation_rate": [0.3, 0.5]  # Her zaman aralığında aktif müşteri oranı
}

# Logger oluştur
logger = get_logger(__name__)

def generate_full_year_data(spark):
    """
    Gerçekçi tam yıl akıllı sayaç verisi üret
    
    Args:
        spark: Spark session
        
    Returns:
        tuple: (DataFrame, record_count)
    """
    logger.info("🏭 GERÇEK DAĞILIMLI TAM YIL AKILLI SAYAÇ VERİSİ ÜRETİLİYOR...")
    start_time = time.time()
    
    # Veri üretim parametrelerini al (config.py'dan)
    start_date = GENERATOR_CONFIG["start_date"]
    end_date = GENERATOR_CONFIG["end_date"]
    time_interval = GENERATOR_CONFIG["time_interval"]
    customer_count = GENERATOR_CONFIG["customer_count"]
    customer_prefix = GENERATOR_CONFIG["customer_id_prefix"]
    
    # Tarih aralığı ve müşteri listesi oluştur
    dates = pd.date_range(start=start_date, end=end_date, freq=time_interval)
    customers = [f"{customer_prefix}{i:04d}" for i in range(1, customer_count + 1)]
    
    logger.info(f"📅 Üretiliyor: {len(dates):,} zaman damgası × {len(customers)} müşteri")
    logger.info(f"⏱️  Beklenen kayıt: ~{len(dates) * len(customers) * 0.3:,.0f} (her zaman damgası için %30 katılım)")
    
    data = []
    
    for i, timestamp in enumerate(dates):
        # Her zaman damgası: 30-50 rastgele müşteri (gerçekçi katılım)
        min_rate, max_rate = GENERATOR_CONFIG["participation_rate"]
        num_active = np.random.randint(int(len(customers) * min_rate), int(len(customers) * max_rate) + 1)
        active_customers = np.random.choice(customers, size=num_active, replace=False)
        
        for customer in active_customers:
            # Gerçekçi yük örüntüleri
            hour = timestamp.hour
            month = timestamp.month
            day_of_week = timestamp.weekday()
            
            # Taban tüketim
            base_load = GENERATOR_CONFIG["base_load"]
            
            # Mevsimsel örüntüler (kWh benzeri ölçekleme)
            if month in [12, 1, 2]:  # Kış
                seasonal = 1.4
            elif month in [6, 7, 8]:  # Yaz  
                seasonal = 1.3
            else:  # İlkbahar/Sonbahar
                seasonal = 0.9
            
            # Günlük örüntüler
            if 6 <= hour <= 9 or 17 <= hour <= 22:  # Pik saatler
                daily = 1.5
            elif 23 <= hour or hour <= 5:  # Gece
                daily = 0.5
            else:  # Pik olmayan
                daily = 1.0
            
            # Hafta sonu vs hafta içi
            weekend_factor = 0.85 if day_of_week >= 5 else 1.0
            
            # Rastgele varyasyon
            noise = np.random.normal(0, GENERATOR_CONFIG["random_noise_std"])
            
            # Son yük hesaplaması
            load = base_load * seasonal * daily * weekend_factor + noise
            load = max(GENERATOR_CONFIG["min_load"], min(GENERATOR_CONFIG["max_load"], load))  # Gerçekçi sınırlar
            
            data.append({
                'customer_id': str(customer),  # ← Açık string dönüşümü
                'full_timestamp': timestamp,
                'load_percentage': float(load)  # ← Açık float dönüşümü
            })
        
        # İlerleme takibi
        if i % 5000 == 0 or i == len(dates) - 1:
            logger.info(f"   {i:,}/{len(dates):,} zaman damgası üretildi ({i/len(dates)*100:.1f}%)")
    
    # Spark DataFrame'e dönüştür
    logger.info(f"📊 {len(data):,} kayıt Spark DataFrame'e dönüştürülüyor...")
    pandas_df = pd.DataFrame(data)
    
    # ✅ Açık tip dönüşümü
    pandas_df['customer_id'] = pandas_df['customer_id'].astype(str)
    pandas_df['load_percentage'] = pandas_df['load_percentage'].astype(float)
    pandas_df['full_timestamp'] = pd.to_datetime(pandas_df['full_timestamp'])
    
    # Açık şema ile Spark DataFrame oluştur
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("full_timestamp", TimestampType(), True),
        StructField("load_percentage", DoubleType(), True)
    ])
    
    spark_df = spark.createDataFrame(pandas_df, schema=schema)
    
    # Önbelleğe al
    spark_df.cache()
    
    # Sonuç istatistikleri
    generation_time = time.time() - start_time
    final_count = spark_df.count()
    
    logger.info(f"✅ TAM YIL VERİSİ ÜRETİLDİ!")
    logger.info(f"📊 Toplam kayıt: {final_count:,}")
    logger.info(f"⏱️  Üretim süresi: {generation_time:.1f}s")
    logger.info(f"📈 Üretim hızı: {final_count/generation_time:,.0f} kayıt/saniye")
    
    # Örnek göster
    logger.info("📋 Örnek veri:")
    spark_df.show(5, truncate=False)
    
    # Tarih aralığı göster
    from pyspark.sql.functions import min as spark_min, max as spark_max
    date_stats = spark_df.select(
        spark_min("full_timestamp").alias("start_date"),
        spark_max("full_timestamp").alias("end_date")
    ).collect()[0]
    
    logger.info(f"📅 Tarih aralığı: {date_stats['start_date']} - {date_stats['end_date']}")
    
    return spark_df, final_count