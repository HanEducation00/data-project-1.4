#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loglama Yöneticisi

Bu modül, ML pipeline'ın çalışması sırasında neler olduğunu kaydetmek,
hataları izlemek ve işlem durumunu takip etmek için kullanılır.
"""

import logging
import os
from datetime import datetime

# Log seviyesi mapping
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

class MLPipelineLogger:
    """ML Pipeline için özel logger sınıfı"""
    
    def __init__(self, name, log_level='INFO', log_to_file=True, log_dir='/workspace/logs'):
        self.name = name
        self.log_level = LOG_LEVELS.get(log_level.upper(), logging.INFO)
        self.log_to_file = log_to_file
        self.log_dir = log_dir
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """Logger'ı yapılandır"""
        # Logger oluştur
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)
        
        # Eğer handler'lar zaten varsa, tekrar ekleme
        if self.logger.handlers:
            return
        
        # Formatter oluştur (emoji desteği ile)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (opsiyonel)
        if self.log_to_file:
            self._add_file_handler(formatter)
    
    def _add_file_handler(self, formatter):
        """Dosya handler'ı ekle"""
        try:
            # Log dizinini oluştur
            os.makedirs(self.log_dir, exist_ok=True)
            
            # Log dosya adı
            today = datetime.now().strftime('%Y-%m-%d')
            log_filename = f"ml_pipeline_{today}.log"
            log_filepath = os.path.join(self.log_dir, log_filename)
            
            # File handler
            file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            self.info(f"📄 Log dosyası: {log_filepath}")
            
        except Exception as e:
            self.warning(f"⚠️ Log dosyası oluşturulamadı: {e}")
    
    def debug(self, message):
        """Debug seviyesi log"""
        self.logger.debug(message)
    
    def info(self, message):
        """Info seviyesi log"""
        self.logger.info(message)
    
    def warning(self, message):
        """Warning seviyesi log"""
        self.logger.warning(message)
    
    def error(self, message):
        """Error seviyesi log"""
        self.logger.error(message)
    
    def critical(self, message):
        """Critical seviyesi log"""
        self.logger.critical(message)
    
    def log_dataframe_info(self, df, name="DataFrame"):
        """DataFrame hakkında bilgi logla"""
        try:
            count = df.count()
            columns = df.columns
            self.info(f"📊 {name}: {count:,} kayıt, {len(columns)} kolon")
            self.debug(f"🔍 {name} kolonları: {columns}")
        except Exception as e:
            self.error(f"❌ {name} bilgisi alınamadı: {e}")
    
    def log_feature_info(self, feature_columns, importance_dict=None):
        """Özellik bilgilerini logla"""
        try:
            self.info(f"🧩 Toplam özellik sayısı: {len(feature_columns)}")
            
            # En önemli özellikleri logla
            if importance_dict and len(importance_dict) > 0:
                sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
                self.info("🔝 En önemli 5 özellik:")
                for i, (feature, score) in enumerate(sorted_features[:5], 1):
                    self.info(f"   {i}. {feature} = {score:.4f}")
        except Exception as e:
            self.error(f"❌ Özellik bilgisi loglama hatası: {e}")
    
    def log_model_metrics(self, metrics):
        """Model metriklerini logla"""
        try:
            self.info("📏 MODEL METRİKLERİ:")
            
            # Eğitim metrikleri
            if "train_r2" in metrics:
                self.info(f"📚 Eğitim: R² = {metrics['train_r2']:.4f}, RMSE = {metrics['train_rmse']:.2f}, MAE = {metrics['train_mae']:.2f}")
            
            # Doğrulama metrikleri
            if "val_r2" in metrics:
                self.info(f"🔍 Doğrulama: R² = {metrics['val_r2']:.4f}, RMSE = {metrics['val_rmse']:.2f}, MAE = {metrics['val_mae']:.2f}")
            
            # Test metrikleri
            if "test_r2" in metrics:
                self.info(f"🧪 Test: R² = {metrics['test_r2']:.4f}, RMSE = {metrics['test_rmse']:.2f}, MAE = {metrics['test_mae']:.2f}")
                
            # Eğitim süresi
            if "training_time" in metrics:
                self.info(f"⏱️ Eğitim süresi: {metrics['training_time']:.1f}s")
        except Exception as e:
            self.error(f"❌ Model metrikleri loglama hatası: {e}")
    
    def log_mlflow_info(self, run_id, experiment_name):
        """MLflow bilgilerini logla"""
        self.info(f"🚀 MLflow Run ID: {run_id}")
        self.info(f"🧪 MLflow Deney: {experiment_name}")
        self.info(f"🔗 MLflow UI: http://mlflow-server:5000")
    
    def log_processing_start(self, operation_name):
        """İşlem başlangıcını logla"""
        self.info(f"🚀 === {operation_name} BAŞLADI ===")
    
    def log_processing_end(self, operation_name, duration_seconds=None):
        """İşlem sonunu logla"""
        if duration_seconds:
            self.info(f"✅ === {operation_name} TAMAMLANDI ({duration_seconds:.2f}s) ===")
        else:
            self.info(f"✅ === {operation_name} TAMAMLANDI ===")
    
    def log_error_with_details(self, error, context=""):
        """Detaylı hata logu"""
        import traceback
        error_details = traceback.format_exc()
        self.error(f"❌ HATA {context}: {str(error)}")
        self.debug(f"Hata detayları:\n{error_details}")
    
    def log_config_info(self, config_dict, config_name="Konfigürasyon"):
        """Konfigürasyon bilgilerini logla"""
        self.info(f"⚙️ {config_name} yüklendi:")
        for key, value in config_dict.items():
            # Şifre alanlarını gizle
            if 'password' in key.lower() or 'pass' in key.lower():
                self.info(f"  {key}: ***")
            else:
                self.info(f"  {key}: {value}")

# Global logger instance'ları
_loggers = {}

def get_logger(name, log_level='INFO', log_to_file=True):
    """
    Logger instance'ı getir (singleton pattern)
    
    Args:
        name (str): Logger adı
        log_level (str): Log seviyesi
        log_to_file (bool): Dosyaya log yazılsın mı
        
    Returns:
        MLPipelineLogger: Logger instance'ı
    """
    if name not in _loggers:
        _loggers[name] = MLPipelineLogger(name, log_level, log_to_file)
    
    return _loggers[name]

def set_global_log_level(log_level):
    """Tüm logger'ların log seviyesini değiştir"""
    level = LOG_LEVELS.get(log_level.upper(), logging.INFO)
    
    for logger in _loggers.values():
        logger.logger.setLevel(level)
        for handler in logger.logger.handlers:
            handler.setLevel(level)

def log_pipeline_start():
    """Pipeline başlangıcını logla"""
    logger = get_logger('PIPELINE')
    logger.info("=" * 60)
    logger.info("🚀 ENERGY FORECASTING ML PIPELINE BAŞLADI")
    logger.info(f"⏱️ Zaman: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

def log_pipeline_end():
    """Pipeline sonunu logla"""
    logger = get_logger('PIPELINE')
    logger.info("=" * 60)
    logger.info("✅ ENERGY FORECASTING ML PIPELINE TAMAMLANDI")
    logger.info(f"⏱️ Zaman: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)