#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Yardımcı Fonksiyonları

Bu modül Spark session yönetimi ve veri işleme yardımcı fonksiyonlarını içerir.
Mevcut işlevselliği korurken, yeni yapılandırma ve loglama sistemiyle entegre eder.
"""

from pyspark.sql import SparkSession

from utils.logger import get_logger
from utils.config import SPARK_CONFIG
from utils.connections import get_spark_session

# Logger oluştur
logger = get_logger(__name__)

def create_spark_session():
    """
    Spark session oluştur
    
    Returns:
        SparkSession: Yapılandırılmış Spark session
    """
    logger.info("⚡ SPARK SESSION OLUŞTURULUYOR...")
    
    # Mevcut bağlantı fonksiyonunu kullan
    spark = get_spark_session()
    
    logger.info(f"✅ Spark session başarıyla oluşturuldu: {spark.version}")
    return spark

def cache_dataframe(df, name="DataFrame"):
    """
    DataFrame'i önbelleğe al ve istatistiklerini göster
    
    Args:
        df: Spark DataFrame
        name: DataFrame adı (loglama için)
        
    Returns:
        DataFrame: Önbelleğe alınmış DataFrame
    """
    logger.info(f"💾 {name} önbelleğe alınıyor...")
    
    try:
        # DataFrame'i önbelleğe al
        df.cache()
        
        # İstatistikler
        count = df.count()
        columns = len(df.columns)
        partitions = df.rdd.getNumPartitions()
        
        logger.info(f"✅ {name} önbelleğe alındı: {count:,} satır, {columns} kolon, {partitions} partition")
        return df
        
    except Exception as e:
        logger.error(f"❌ {name} önbelleğe alınırken hata: {e}")
        return df

def show_dataframe_info(df, name="DataFrame", sample_rows=5):
    """
    DataFrame hakkında bilgi göster
    
    Args:
        df: Spark DataFrame
        name: DataFrame adı
        sample_rows: Gösterilecek örnek satır sayısı
    """
    logger.info(f"📊 {name.upper()} BİLGİLERİ:")
    
    try:
        # Temel bilgiler
        count = df.count()
        columns = df.columns
        
        logger.info(f"📏 Toplam kayıt: {count:,}")
        logger.info(f"🧩 Kolonlar: {len(columns)}")
        
        # Örnek veri
        logger.info(f"📋 Örnek veriler:")
        df.show(sample_rows, truncate=False)
        
    except Exception as e:
        logger.error(f"❌ DataFrame bilgisi gösterilirken hata: {e}")

def unpersist_dataframe(df, name="DataFrame"):
    """
    DataFrame'i önbellekten çıkar
    
    Args:
        df: Spark DataFrame
        name: DataFrame adı (loglama için)
        
    Returns:
        DataFrame: Önbellekten çıkarılmış DataFrame
    """
    logger.info(f"🧹 {name} önbellekten çıkarılıyor...")
    
    try:
        df.unpersist()
        logger.info(f"✅ {name} önbellekten çıkarıldı")
        return df
    except Exception as e:
        logger.error(f"❌ {name} önbellekten çıkarılırken hata: {e}")
        return df