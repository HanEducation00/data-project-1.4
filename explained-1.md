### SRC

2-src/
└── pipelines/
    └── 1-local_raw_to_db/         # Yerel ham veriyi DB'ye aktaran pipeline
        ├── __init__.py            # Python paket tanımı
        ├── main.py                # Ana çalıştırma dosyası
        ├── schemas.py             # Veri şemaları
        ├── data_processing/       # Veri işleme modülleri
        │   ├── __init__.py        # Python paket tanımı
        │   ├── parser.py          # Dosya okuma ve parse etme
        │   └── processor.py       # Ana veri işleme
        └── utils/                 # Yardımcı fonksiyonlar
            ├── __init__.py        # Python paket tanımı
            ├── config.py          # Konfigürasyon ayarları
            ├── connections.py     # DB ve Spark bağlantıları
            └── logger.py          # Loglama sistemi


config.py → Tüm bağlantı bilgileri ve yapılandırmalar burada tanımlanır
connections.py → Spark başlatma/durdurma, veritabanı bağlantısı, tablo kontrolleri
parser.py → Yerel dosyalardan veri okuma ve yapılandırma

Ancak gerçek akış biraz daha karmaşıktır:
main.py → processor.py (run_pipeline) → connections.py → parser.py → veritabanına yazma
Aslında, processor.py anahtar dosyadır ve tüm işlemi koordine eder:

main.py pipeline'ı başlatır
processor.py içindeki run_pipeline() çağrılır:

Bağlantıları test eder (connections.py kullanarak)
Tabloları kontrol eder/oluşturur
Spark session'ı alır
Dosyaları bulur ve parse eder (parser.py veya kendi içindeki parsing fonksiyonları ile)
İşlenmiş verileri veritabanına yazar



İlginç bir nokta: processor.py dosyası, bazı parsing fonksiyonlarını kendi içinde de tanımlar ve bunlar parser.py'daki fonksiyonlara çok benzer (bazı optimizasyonlarla). Bu durum kod tekrarına neden olmuş, ama işlevsellik açısından değişen bir şey yok.
Sonuç olarak genel akış:
Konfigürasyon → Bağlantılar → Veri Okuma/İşleme → Veritabanına Yazma            