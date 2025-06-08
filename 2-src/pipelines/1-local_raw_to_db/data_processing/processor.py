#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri İşleme Ana Modülü

Bu modül pipeline akışını yönetir ve veri zenginleştirme işlemlerini gerçekleştirir.
"""

import time
from datetime import datetime, timedelta 

# Utility modülleri
from ..utils.config import DATA_CONFIG, TABLE_NAME
from ..utils.logger import get_logger
from ..utils.connections import (
    get_spark_session, test_postgresql_connection, 
    table_exists, write_dataframe_to_postgresql,
    create_table_if_not_exists, cleanup_connections
)
from .parser import parse_multiple_files, get_sample_day_files


# Şemalar
from ..schemas import POSTGRES_TABLE_SCHEMA
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType

logger = get_logger(__name__)

def run_pipeline():
    """
    Ana pipeline işlemi
    
    Returns:
        bool: Başarılı ise True
    """
    logger.log_processing_start("LOCAL RAW TO DB PIPELINE")
    start_time = time.time()
    
    spark = None
    
    try:
        # 1. Bağlantıları test et
        logger.info("Bağlantılar test ediliyor...")
        if not test_postgresql_connection():
            logger.error("PostgreSQL bağlantısı başarısız!")
            return False
        
        logger.info("Tüm bağlantılar başarılı")
        
        # 2. Tablolar kontrol et ve oluştur
        logger.info("Tablolar kontrol ediliyor...")
        if not table_exists(TABLE_NAME):
            logger.info(f"Tablo bulunamadı: {TABLE_NAME}, oluşturuluyor...")
            
            if create_table_if_not_exists(POSTGRES_TABLE_SCHEMA):
                logger.info(f"Tablo başarıyla oluşturuldu: {TABLE_NAME}")
            else:
                logger.error(f"Tablo oluşturulamadı: {TABLE_NAME}")
                return False
        else:
            logger.info(f"Tablo mevcut: {TABLE_NAME}")
        
        # 3. Spark session al
        spark = get_spark_session()
        
        # 4. Dosyaları bul
        logger.info("Veri dosyaları aranıyor...")
        file_paths = get_sample_day_files()  # parser.py'den gelen fonksiyon
        
        if not file_paths:
            logger.error("Hiç veri dosyası bulunamadı!")
            return False
        
        # 5. Dosyaları parse et
        logger.info("Dosyalar parse ediliyor...")
        parsed_df = parse_multiple_files(spark, file_paths)  # parser.py'den gelen fonksiyon
        
        if parsed_df is None:
            logger.error("Dosya parse işlemi başarısız!")
            return False
        
        # 6. Tam tarih-saat damgası ekle (veri zenginleştirme)
        logger.info("Veriler zenginleştiriliyor (tam tarih ekleniyor)...")
        enriched_df = add_full_timestamps(spark, parsed_df)
        
        # 7. Veritabanına yaz
        logger.info("Veriler PostgreSQL'e yazılıyor...")
        if write_dataframe_to_postgresql(enriched_df, TABLE_NAME, mode="append"):
            logger.info("Pipeline başarıyla tamamlandı!")
            
            # Sonuç istatistikleri
            end_time = time.time()
            duration = end_time - start_time
            logger.log_processing_end("LOCAL RAW TO DB PIPELINE", duration)
            
            return True
        else:
            logger.error("Veritabanına yazma başarısız!")
            return False
        
    except Exception as e:
        logger.log_error_with_details(e, "PIPELINE")
        return False
        
    finally:
        # Spark session'ı temizle
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session kapatıldı")
            except Exception as e:
                logger.warning(f"Spark session kapatma hatası: {e}")
        
        # Diğer bağlantıları temizle
        try:
            cleanup_connections()
        except Exception as e:
            logger.warning(f"Connection cleanup hatası: {e}")


def add_full_timestamps(spark, df):
    """
    DataFrame'e tam tarih-saat damgası ekler
    
    Args:
        spark: Spark session
        df: İşlenmiş DataFrame
    
    Returns:
        DataFrame: Zenginleştirilmiş DataFrame
    """
    logger.info("Tam tarih-saat damgası ekleniyor...")
    
    # UDF (User Defined Function) tanımla
    def calculate_timestamp(day_num, hour, minute):
        if day_num is None or hour is None or minute is None:
            return None
        
        try:
            # Base tarih: 2016-01-01
            base_date = datetime(2016, 1, 1)
            actual_date = base_date + timedelta(days=day_num - 1)
            return actual_date + timedelta(hours=hour, minutes=minute)
        except Exception:
            return None
    
    # UDF'i kaydet
    timestamp_udf = udf(calculate_timestamp, TimestampType())
    
    # DataFrame'e timestamp ekle
    result_df = df.withColumn("full_timestamp", 
                            timestamp_udf(df.day_num, df.hour, df.minute))
    
    logger.info("Tam tarih-saat damgası başarıyla eklendi")
    return result_df