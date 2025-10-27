from pyspark.sql import SparkSession

def data_sampler(spark: SparkSession, desired_sample_size: int):
    # Cel: Wybieramy małe próbki z dużych zbiorów danych, aby znacząco przyspieszyć
    # proces obliczeniowy i umożliwić jego wykonanie w rozsądnym czasie, zamiast
    # przetwarzać miliony rekordów. To uproszczenie jest kluczowe dla testów i analizy pilotażowej.

    # Wczytanie zbiorów danych białek Danio (ryba) i Myszy
    danio = spark.read.parquet("data/danio_protein.adam")
    mysz = spark.read.parquet("data/mysz_protein.adam")

    SEED = 30
    SAMPLING_MARGIN = 0.00005

    danio_count = danio.count()
    mysz_count = mysz.count()
    # Obliczamy frakcję potrzebną do uzyskania żądanej wielkości próbki, dodając mały margines,
    # aby zwiększyć szanse na osiągnięcie pożądanej liczby rekordów.
    danio_fraction = (desired_sample_size / danio_count) + SAMPLING_MARGIN
    mysz_fraction = (desired_sample_size / mysz_count) + SAMPLING_MARGIN

    # Pobieramy próbki bez zastępowania, używając stałego SEED dla powtarzalności.
    danio_sample = danio.sample(withReplacement=False, fraction=danio_fraction, seed=SEED) \
                        .orderBy('referenceName') \
                        .limit(desired_sample_size)
                        
    mysz_sample = mysz.sample(withReplacement=False, fraction=mysz_fraction, seed=SEED) \
                        .orderBy('referenceName') \
                        .limit(desired_sample_size)
    
    return danio_sample, mysz_sample