#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Model Değerlendirme ve Özellik Önemliliği Analizi

Bu modül, eğitilmiş modelin özellik önemliliklerini analiz eder ve görselleştirir.
"""

import numpy as np
import matplotlib.pyplot as plt
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.classification import GBTClassificationModel

from utils.logger import get_logger

# Logger oluştur
logger = get_logger(__name__)

def get_feature_importance(model, feature_columns):
    """
    Model özellik önemliliklerini çıkar ve göster
    
    Args:
        model: Eğitilmiş pipeline modeli
        feature_columns: Özellik kolonu isimleri
        
    Returns:
        dict: Özellik-önemlilik sözlüğü
    """
    logger.info("📈 ÖZELLİK ÖNEMLİLİĞİ ANALİZ EDİLİYOR...")
    
    try:
        # Pipeline'ın son aşaması olan GBT modelini al
        if not hasattr(model, 'stages'):
            logger.warning("⚠️ Model bir pipeline değil, doğrudan özellik önemliliğine erişiliyor")
            gbt_model = model
        else:
            # Son aşama GBT modeli olmalı
            gbt_model = model.stages[-1]
            logger.info(f"✅ Pipeline modelinden GBT modeli alındı: {type(gbt_model).__name__}")
        
        # Modelin özellik önemliliği desteği var mı kontrol et
        if not hasattr(gbt_model, 'featureImportances'):
            logger.error("❌ Model özellik önemliliğini desteklemiyor")
            return {}
        
        # Özellik önemliliklerini çıkar
        importance_scores = gbt_model.featureImportances.toArray()
        
        # Özellik sayısı uyuşuyor mu kontrol et
        if len(importance_scores) != len(feature_columns):
            logger.warning(f"⚠️ Özellik sayısı uyuşmuyor: {len(importance_scores)} skor vs {len(feature_columns)} özellik")
            
            # Eğer özellik sayısı az ise kırpma işlemi yap
            if len(importance_scores) < len(feature_columns):
                logger.warning(f"⚠️ Özellik listesi kırpılıyor: {len(feature_columns)} -> {len(importance_scores)}")
                feature_columns = feature_columns[:len(importance_scores)]
            else:
                logger.warning(f"⚠️ Özellik skorları kırpılıyor: {len(importance_scores)} -> {len(feature_columns)}")
                importance_scores = importance_scores[:len(feature_columns)]
        
        # Özellik-önemlilik sözlüğü oluştur
        importance_dict = dict(zip(feature_columns, importance_scores))
        
        # Önemliliğe göre sırala
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Üst 10 özelliği göster
        logger.info(f"🔝 EN ÖNEMLİ 10 ÖZELLİK:")
        for i, (feature, score) in enumerate(sorted_features[:10], 1):
            logger.info(f"   {i:2d}. {feature:<25} = {score:.4f}")
        
        # Toplam önemlilik ve istatistikler
        total_importance = sum(importance_scores)
        top10_importance = sum(score for _, score in sorted_features[:10])
        
        logger.info(f"📊 ÖZELLİK ÖNEMLİLİĞİ İSTATİSTİKLERİ:")
        logger.info(f"   Toplam özellik sayısı: {len(feature_columns)}")
        logger.info(f"   İlk 10 özelliğin toplam önemliliği: {top10_importance:.4f} ({top10_importance/total_importance*100:.1f}%)")
        
        # Özellik önemliliği görselleştirme
        try:
            create_importance_plot(sorted_features[:15], "feature_importance.png")
        except Exception as viz_error:
            logger.warning(f"⚠️ Görselleştirme oluşturulamadı: {viz_error}")
        
        return importance_dict
        
    except Exception as e:
        logger.error(f"❌ Özellik önemliliği çıkarılamadı: {e}")
        import traceback
        logger.debug(f"Hata detayı: {traceback.format_exc()}")
        return {}

def create_importance_plot(sorted_features, filename=None):
    """
    Özellik önemliliği grafiği oluştur
    
    Args:
        sorted_features: Önemliliğe göre sıralanmış (özellik, skor) çiftleri
        filename: Kaydedilecek dosya adı (None ise kaydetmez)
    """
    # Veriyi hazırla
    features = [item[0] for item in sorted_features]
    scores = [item[1] for item in sorted_features]
    
    # Özellik isimlerini kısalt (çok uzunsa)
    features = [f[:20] + "..." if len(f) > 20 else f for f in features]
    
    # Grafiği oluştur
    plt.figure(figsize=(10, 8))
    bars = plt.barh(range(len(features)), scores, align='center', color='skyblue')
    plt.yticks(range(len(features)), features)
    plt.xlabel('Önemlilik Skoru')
    plt.title('Özellik Önemliliği')
    
    # Skor değerlerini ekle
    for i, bar in enumerate(bars):
        plt.text(bar.get_width() + 0.002, bar.get_y() + bar.get_height()/2, 
                f'{scores[i]:.4f}', va='center')
    
    plt.tight_layout()
    
    # Dosyaya kaydet
    if filename:
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        logger.info(f"📊 Özellik önemliliği grafiği kaydedildi: {filename}")
    
    plt.close()

def analyze_feature_groups(importance_dict, feature_columns):
    """
    Özellik gruplarına göre önemlilik analizi
    
    Args:
        importance_dict: Özellik-önemlilik sözlüğü
        feature_columns: Özellik kolonu isimleri
        
    Returns:
        dict: Grup bazında toplam önemlilik
    """
    logger.info("🔍 ÖZELLİK GRUPLARI ANALİZ EDİLİYOR...")
    
    # Özellik gruplarını tanımla
    groups = {
        "lag": [f for f in feature_columns if 'lag' in f],
        "rolling": [f for f in feature_columns if 'rolling' in f],
        "time": [f for f in feature_columns if any(x in f for x in ['sin', 'cos', 'month', 'day'])],
        "categorical": [f for f in feature_columns if f.startswith('is_')],
        "trend": [f for f in feature_columns if 'trend' in f]
    }
    
    # Diğer özellikler
    all_grouped = [f for group in groups.values() for f in group]
    groups["other"] = [f for f in feature_columns if f not in all_grouped]
    
    # Grup bazında önemlilik toplamları
    group_importance = {}
    total_importance = sum(importance_dict.values())
    
    for group_name, group_features in groups.items():
        if not group_features:
            continue
            
        group_sum = sum(importance_dict.get(f, 0) for f in group_features)
        group_pct = group_sum / total_importance * 100
        group_importance[group_name] = (group_sum, group_pct, len(group_features))
        
        logger.info(f"   {group_name.upper()} özellikleri ({len(group_features)}): {group_sum:.4f} ({group_pct:.1f}%)")
    
    return group_importance