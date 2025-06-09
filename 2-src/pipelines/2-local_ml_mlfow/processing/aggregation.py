#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Toplama İşlemleri

Bu modül, ham akıllı sayaç verilerini sistem seviyesinde toplar ve
günlük toplam enerji tüketimi hesaplar.
"""

import time
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min,
    count as spark_count, date_format, hour, dayofweek, month, year
)
from pyspark.sql.types import TimestampType

from utils.logger import get_logger
from utils.config import PREPROCESSING_CONFIG
from utils.spark_utils import cache_dataframe, show_dataframe_info

# Logger oluştur
logger = get_logger(__name__)

def create_system_level_aggregation(df):
    """
    Müşteri verilerini sistem seviyesine topla (zaman damgası başına)
    
    Args:
        df: Ham veri DataFrame'i
        
    Returns:
        tuple: (system_df, processing_time)
    """
    logger.info("🔧 SİSTEM SEVİYESİ TOPLAMA OLUŞTURULUYOR...")
    start_time = time.time()
    
    # Zaman damgasına göre grupla ve tüm müşterileri topla
    system_df = df.groupBy("full_timestamp") \
        .agg(
            spark_sum("load_percentage").alias("total_system_load"),
            spark_avg("load_percentage").alias("avg_system_load"),
            spark_max("load_percentage").alias("max_system_load"),
            spark_min("load_percentage").alias("min_system_load"),
            spark_count("customer_id").alias("active_customers")
        )
    
    # Zaman bazlı özellikler ekle
    system_df = system_df \
        .withColumn("date", date_format("full_timestamp", "yyyy-MM-dd")) \
        .withColumn("hour", hour("full_timestamp")) \
        .withColumn("month", month("full_timestamp")) \
        .withColumn("year", year("full_timestamp")) \
        .withColumn("dayofweek", dayofweek("full_timestamp"))
    
    # İstatistikleri hesaplamadan önce önbelleğe al
    system_df = cache_dataframe(system_df, "Sistem Seviyesi Veri")
    
    # İşleme süresini ve kayıt sayısını hesapla
    processing_time = time.time() - start_time
    record_count = system_df.count()
    
    # İşleme metrikleri
    logger.info(f"✅ Sistem toplama işlemi {processing_time:.1f}s sürede tamamlandı")
    logger.info(f"📊 Sistem kayıtları: {record_count:,} zaman damgası")
    
    # Verilerin dağılımını analiz et
    load_stats = system_df.select(
        spark_avg("total_system_load").alias("avg_load"),
        spark_min("total_system_load").alias("min_load"),
        spark_max("total_system_load").alias("max_load"),
        spark_avg("active_customers").alias("avg_customers")
    ).collect()[0]
    
    logger.info(f"📈 SİSTEM YÜKÜ İSTATİSTİKLERİ:")
    logger.info(f"   Ortalama yük: {load_stats['avg_load']:.2f}")
    logger.info(f"   Minimum yük: {load_stats['min_load']:.2f}")
    logger.info(f"   Maksimum yük: {load_stats['max_load']:.2f}")
    logger.info(f"   Ortalama aktif müşteri: {load_stats['avg_customers']:.1f}")
    
    # Örnek veri göster
    logger.info("📋 Örnek sistem verisi:")
    system_df.select("full_timestamp", "total_system_load", "avg_system_load", "active_customers").show(5)
    
    return system_df, processing_time

def create_daily_total_energy(system_df):
    """
    Günlük toplam enerji tüketimi hesapla (gün başına tüm zaman damgalarını topla)
    
    Args:
        system_df: Sistem seviyesi DataFrame
        
    Returns:
        tuple: (daily_df, day_count, processing_time)
    """
    logger.info("📊 GÜNLÜK TOPLAM ENERJİ OLUŞTURULUYOR...")
    start_time = time.time()
    
    # Tarihe göre grupla ve o gün için tüm zaman damgalarını topla
    daily_df = system_df.groupBy("date") \
        .agg(
            spark_sum("total_system_load").alias("total_daily_energy"),  # Ana hedef!
            spark_avg("total_system_load").alias("avg_daily_load"),
            spark_max("total_system_load").alias("peak_daily_load"),
            spark_min("total_system_load").alias("min_daily_load"),
            spark_sum("active_customers").alias("total_daily_customers"),
            spark_avg("active_customers").alias("avg_daily_customers")
        )
    
    # Tarih özellikleri ekle
    daily_df = daily_df \
        .withColumn("timestamp", col("date").cast(TimestampType())) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("dayofweek", dayofweek(col("timestamp")))
    
    # Tarihe göre sırala
    daily_df = daily_df.orderBy("date")
    
    # Yeniden bölümle ve önbelleğe al (daha iyi performans için)
    if "repartition_count" in PREPROCESSING_CONFIG:
        partition_count = PREPROCESSING_CONFIG["repartition_count"]
        logger.info(f"🔄 Günlük veri {partition_count} partitiona yeniden bölümleniyor...")
        daily_df = daily_df.repartition(partition_count)
    
    # Önbelleğe al
    daily_df = cache_dataframe(daily_df, "Günlük Toplam Enerji")
    
    # İşlem süresini ve gün sayısını hesapla
    processing_time = time.time() - start_time
    day_count = daily_df.count()
    
    logger.info(f"✅ Günlük toplama işlemi {processing_time:.1f}s sürede tamamlandı")
    logger.info(f"📊 Toplam gün: {day_count}")
    
    # Günlük veriler hakkında detaylı istatistikler
    logger.info("📋 Örnek günlük veri:")
    daily_df.select("date", "total_daily_energy", "avg_daily_load", "peak_daily_load").show(10)
    
    # Enerji istatistiklerini göster
    energy_stats = daily_df.select(
        spark_avg("total_daily_energy").alias("avg_daily_energy"),
        spark_min("total_daily_energy").alias("min_daily_energy"),
        spark_max("total_daily_energy").alias("max_daily_energy"),
        (spark_max("total_daily_energy") / spark_min("total_daily_energy")).alias("max_min_ratio")
    ).collect()[0]
    
    logger.info(f"📈 GÜNLÜK ENERJİ İSTATİSTİKLERİ:")
    logger.info(f"   Ortalama: {energy_stats['avg_daily_energy']:,.1f}")
    logger.info(f"   Minimum: {energy_stats['min_daily_energy']:,.1f}")
    logger.info(f"   Maksimum: {energy_stats['max_daily_energy']:,.1f}")
    logger.info(f"   Maks/Min oranı: {energy_stats['max_min_ratio']:,.2f}")
    
    # Aylık özete göre toplam enerjiyi göster
    logger.info("📅 AYLIK ENERJİ DAĞILIMI:")
    monthly_stats = daily_df.groupBy("month") \
        .agg(
            spark_sum("total_daily_energy").alias("monthly_energy"),
            spark_avg("total_daily_energy").alias("avg_daily_energy"),
            spark_count("date").alias("day_count")
        ) \
        .orderBy("month")
    
    monthly_stats.show()
    
    return daily_df, day_count, processing_time