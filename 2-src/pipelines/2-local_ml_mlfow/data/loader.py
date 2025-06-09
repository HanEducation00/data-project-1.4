#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Veri Yükleme Modülü

Bu modül akıllı sayaç verilerini yerel dosyalardan yükler.
"""

import os
from pyspark.sql.functions import col, regexp_extract, to_timestamp
from pyspark.sql.types import StringType, DoubleType, TimestampType

from utils.logger import get_logger
from utils.config import DATA_CONFIG

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, TimestampType, FloatType, IntegerType
# Logger oluştur
logger = get_logger(__name__)

def load_smart_meter_data(spark):
    """
    Akıllı sayaç verilerini yerel dosyalardan yükle
    
    Args:
        spark: Spark session
        
    Returns:
        tuple: (DataFrame, record_count)
    """
    logger.info("📁 AKILLI SAYAÇ VERİLERİ YÜKLENİYOR...")
    
    # Ana veri yolu
    base_path = DATA_CONFIG["base_path"]
    file_pattern = DATA_CONFIG["file_pattern"]
    data_path = f"{base_path}/{file_pattern}"
    
    logger.info(f"🔍 Ana yol kontrol ediliyor: {base_path}")
    
    # Ana yol var mı kontrol et
    if os.path.exists(base_path) and len(os.listdir(base_path)) > 0:
        logger.info(f"✅ Yerel akıllı sayaç dosyaları bulundu: {base_path}")
        return load_local_files(spark, data_path)
    else:
        # Alternatif yolları dene
        for alt_path in DATA_CONFIG["alt_paths"]:
            logger.info(f"🔍 Alternatif yol kontrol ediliyor: {alt_path}")
            
            if os.path.exists(alt_path) and len(os.listdir(alt_path)) > 0:
                alt_data_path = f"{alt_path}/{file_pattern}"
                logger.info(f"✅ Alternatif yolda dosyalar bulundu: {alt_path}")
                return load_local_files(spark, alt_data_path)
        
        # Hiçbir yerde dosya bulunamadı
        logger.error("❌ Hiçbir veri dosyası bulunamadı!")
        raise FileNotFoundError("Veri dosyaları bulunamadı. Lütfen veri yollarını kontrol edin.")

def load_local_files(spark, data_path):
    """
    Gerçek akıllı sayaç txt dosyalarını yükle
    
    Args:
        spark: Spark session
        data_path: Dosya yolu pattern
        
    Returns:
        tuple: (DataFrame, record_count)
    """
    logger.info(f"📂 Yükleniyor: {data_path}")
    
    try:
        # Önce bir dosyayı örnek olarak inceleyip yapısını anla
        sample_files = spark.sparkContext.wholeTextFiles(data_path, 1).take(1)
        
        if not sample_files:
            logger.error(f"❌ Veri yolunda dosya bulunamadı: {data_path}")
            raise FileNotFoundError(f"Belirtilen yolda dosya bulunamadı: {data_path}")
        
        # Örnek dosyanın içeriğini incele
        sample_content = sample_files[0][1]
        logger.info(f"📝 Örnek dosya inceleniyor...")
        
        # Dosya formatını algıla
        if "FORMAT=" in sample_content:
            logger.info("✅ Özel formatlı veri dosyası tespit edildi (FORMAT= içeriyor)")
            # Dosyaları özel formatta oku - FORMAT= içeren dosyalar
            return load_formatted_files(spark, data_path)
        else:
            # Standart CSV olarak oku
            logger.info("📄 Standart CSV formatında okuma deneniyor...")
            return load_csv_files(spark, data_path)
    
    except Exception as e:
        logger.error(f"❌ Yerel dosyaları yükleme hatası: {e}")
        raise RuntimeError(f"Veri yükleme hatası: {e}")

def load_formatted_files(spark, data_path):
    """
    Özel formatlı dosyaları yükle (FORMAT= içeren dosyalar)
    
    Args:
        spark: Spark session
        data_path: Dosya yolu pattern
        
    Returns:
        tuple: (DataFrame, record_count)
    """
    logger.info("📊 Özel formatlı dosyaları işleme...")
    
    try:
        # Dosyaları metin olarak oku
        raw_df = spark.read.text(data_path)
        
        # Veri satırlarını filtrele (res_ ile başlayanlar)
        data_lines = raw_df.filter(col("value").startswith("res_"))
        
        # Örnek veri satırını al
        sample_line = data_lines.first()
        if sample_line:
            logger.info(f"📋 Örnek veri satırı: {sample_line['value'][:100]}...")
        
        # Explode işlemi için yardımcı fonksiyon
        def parse_and_explode(line):
            parts = line.split(',')
            if len(parts) < 12:  # En az 11 meta veri alanı + 1 değer olmalı
                return []
                
            customer_id = parts[0].replace("res_", "")
            profile_type = parts[1]
            year = parts[6]
            month_str = parts[7]
            day = parts[8]
            
            # Ay değerini sayısal değere dönüştür
            month_map = {
                "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
                "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
                "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12
            }
            month = month_map.get(month_str, 1)
            
            # Yük değerlerini al (12. sütundan itibaren)
            load_values = parts[11:]
            
            # Her değer için satır oluştur
            result_rows = []
            for i, load_value in enumerate(load_values):
                if not load_value or load_value.upper() in ['NONE', 'NULL']:
                    continue
                
                # 15 dakikalık aralıkları saat ve dakikaya dönüştür
                hour = (i * 15) // 60
                minute = (i * 15) % 60
                
                # Zaman damgası oluştur
                from datetime import datetime
                timestamp = datetime(int(year), month, int(day), hour, minute)
                
                try:
                    # Yük değerini float'a dönüştür
                    load_pct = float(load_value)
                    
                    # Satır oluştur
                    result_rows.append({
                        'customer_id': customer_id,
                        'profile_type': profile_type,
                        'full_timestamp': timestamp,
                        'load_percentage': load_pct,
                        'day_num': int(day),
                        'hour': hour,
                        'minute': minute,
                        'month': month,
                        'year': int(year)
                    })
                except ValueError:
                    # Sayısal olmayan değerleri atla
                    pass
                    
            return result_rows
            

        # Çıktı şeması tanımla
        output_schema = ArrayType(StructType([
            StructField("customer_id", StringType(), True),
            StructField("profile_type", StringType(), True),
            StructField("full_timestamp", TimestampType(), True),
            StructField("load_percentage", FloatType(), True),
            StructField("day_num", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("minute", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("year", IntegerType(), True)
        ]))
        
        # UDF'i kaydet
        from pyspark.sql.functions import udf, explode
        parse_udf = udf(parse_and_explode, output_schema)
        
        # Satırları parse et ve explode ile her değeri ayrı satıra çıkar
        parsed_df = data_lines.select(
            explode(parse_udf(col("value"))).alias("parsed")
        ).select("parsed.*")
        
        # Önbelleğe al
        parsed_df.cache()
        count = parsed_df.count()
        
        logger.info(f"✅ Özel format verisi yüklendi: {count:,} kayıt")
        
        # Örnek veri
        logger.info("📋 Örnek veri:")
        parsed_df.show(5, truncate=False)
        
        return parsed_df, count
        
    except Exception as e:
        logger.error(f"❌ Özel format verisi yükleme hatası: {e}")
        import traceback
        logger.debug(f"Hata detayları:\n{traceback.format_exc()}")
        raise RuntimeError(f"Özel format verisi yükleme hatası: {e}")

def load_csv_files(spark, data_path):
    """
    Standart CSV formatında dosyaları yükle
    
    Args:
        spark: Spark session
        data_path: Dosya yolu pattern
        
    Returns:
        tuple: (DataFrame, record_count)
    """
    logger.info("📄 Standart CSV formatında yükleme deneniyor...")
    
    try:
        # Önce ham CSV'yi yükle
        raw_df = spark.read.csv(data_path, header=True, inferSchema=True)
        
        # Kolonları göster
        logger.info(f"📋 CSV kolon isimleri: {raw_df.columns}")
        
        # Örnek veri
        logger.info("📋 Örnek ham veri:")
        raw_df.show(5, truncate=False)
        
        # Mevcut kolon isimlerine göre dönüşüm yap
        # NOT: Gerçek kolon isimlerinize göre bu kısmı düzenleyin
        
        df = raw_df
        
        # Önbelleğe al
        df.cache()
        count = df.count()
        
        logger.info(f"✅ CSV verisi yüklendi: {count:,} kayıt")
        
        return df, count
        
    except Exception as e:
        logger.error(f"❌ CSV verisi yükleme hatası: {e}")
        raise RuntimeError(f"CSV verisi yükleme hatası: {e}")