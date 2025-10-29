from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import MinHashLSH
from pyspark.sql import functions as F
from utils.udfs import generate_tripeptides
from utils.data_preparation import data_sampler
import os
import time
import json
utils.config import DESIRED_SAMPLE_SIZE

# --------------------------------------------------------
# Inicjalizacja sesji Spark
# --------------------------------------------------------
# Inicjalizujemy sesję Spark, która jest niezbędna do uruchomienia operacji na dużych
# zbiorach danych i ustawiamy nazwę aplikacji.
spark = SparkSession.builder \
    .appName("Protein MinHash Similarity") \
    .getOrCreate()

# Utworzenie katalogu wyjściowego, jeśli nie istnieje
os.makedirs("output", exist_ok=True)

# Słownik do przechowywania czasów wykonania poszczególnych kroków.
timing_log = {}
start_time_total = time.time()

# ========================================================
# Krok 1: Wczytanie danych i pobranie próbek
# ========================================================
step_start = time.time()
# Cel: MinHash LSH jest wydajny na dużych zbiorach, ale do demonstracji lub testów
# na mniejszej infrastrukturze nadal bierzemy próbkę. To przyspiesza cały potok.

danio_sample, mysz_sample = data_sampler(spark, DESIRED_SAMPLE_SIZE)

timing_log["krok_1_wczytanie_i_probkowanie"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 2: Ekstrakcja sekwencji
# ========================================================
step_start = time.time()
# Cel: Izolujemy tylko kolumny `referenceName` i `sequence`, aby ograniczyć
# pamięć i narzut przetwarzania w kolejnych transformacjach.

danio_sequences = danio_sample.select("referenceName", "sequence")
mysz_sequences = mysz_sample.select("referenceName", "sequence")

timing_log["krok_2_ekstrakcja_sekwencji"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 3: Generowanie 3-merów (trójpeptydów)
# ========================================================
step_start = time.time()
# Cel: Dzielimy długie sekwencje białkowe na krótkie, 3-literowe fragmenty (trójpeptydy).
# MinHash LSH działa na zbiorach elementów, a trójpeptydy stanowią te elementy.

# Rejestracja UDF do generowania trójpeptydów
tripeptides_udf = udf(generate_tripeptides, ArrayType(StringType()))

# Dodanie kolumny z listą trójpeptydów
danio_kmers = danio_sequences.withColumn("tripeptides", tripeptides_udf("sequence"))
mysz_kmers = mysz_sequences.withColumn("tripeptides", tripeptides_udf("sequence"))

timing_log["krok_3_generowanie_trojpeptydow"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 4: Budowanie słownika wszystkich trójpeptydów
# ========================================================
step_start = time.time()
# Cel: Aby przekształcić listę trójpeptydów w Wektor Rzadki (Sparse Vector),
# musimy najpierw stworzyć globalny słownik (leksykon) wszystkich unikalnych
# 3-merów, by móc im przypisać indeksy w wektorze.

# Łączymy wszystkie trójpeptydy z obu zbiorów i zbieramy unikalne elementy do listy
all_tripeptides = danio_kmers.select("tripeptides").union(mysz_kmers.select("tripeptides"))
all_tripeptides_list = all_tripeptides.rdd.flatMap(lambda row: row.tripeptides).distinct().collect()
# Tworzymy mapowanie trójpeptydu na unikalny indeks
tripeptide_to_index = {tripeptide: idx for idx, tripeptide in enumerate(all_tripeptides_list)}
vocab_size = len(tripeptide_to_index)
print(f"Całkowita liczba unikalnych trójpeptydów: {vocab_size}")

timing_log["krok_4_budowanie_slownika"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 5: Konwersja list trójpeptydów na Wektory Rzadkie
# ========================================================
step_start = time.time()
# Cel: MinHashLSH wymaga, aby dane wejściowe były w formacie SparseVector (Wektor Rzadki).
# Każdy wektor jest binarny: 1 oznacza obecność danego trójpeptydu ze słownika, 0 nieobecność.
# Reprezentacja rzadka jest efektywna pamięciowo.

tripeptide_to_index_broadcast = spark.sparkContext.broadcast(tripeptide_to_index)

def tripeptides_to_sparse_vector(tripeptide_list):
    index_map = tripeptide_to_index_broadcast.value
    indices = [index_map[t] for t in set(tripeptide_list) if t in index_map]
    indices.sort()
    values = [1.0] * len(indices)
    return Vectors.sparse(len(index_map), indices, values)

sparse_vector_udf = udf(tripeptides_to_sparse_vector, VectorUDT())

# Dodanie kolumny "features" (cechy) zawierającej Wektor Rzadki
danio_vectors = danio_kmers.withColumn("features", sparse_vector_udf("tripeptides"))
mysz_vectors = mysz_kmers.withColumn("features", sparse_vector_udf("tripeptides"))

timing_log["krok_5_konwersja_na_wektory"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 6: Zastosowanie MinHashLSH
# ========================================================
step_start = time.time()
# Cel: Trenujemy model MinHash LSH. LSH to technika haszowania, która grupuje
# podobne elementy do tych samych "kubełków" (hash tables). Wiele tabel haszujących
# zmniejsza prawdopodobieństwo kolizji dla elementów niepodobnych.

# Tworzymy instancję MinHashLSH
minhash = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
# Trenujemy model na danych Danio. Ten model będzie używany do haszowania obu zbiorów.
model = minhash.fit(danio_vectors)

timing_log["krok_6_dopasowanie_minhash"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 7: Przybliżone łączenie podobieństwa
# ========================================================
step_start = time.time()
# Cel: Wykonujemy przybliżone łączenie podobieństwa (Approximate Similarity Join)
# między zbiorami Danio a Myszy, używając wytrenowanego modelu LSH.
# Algorytm ten jest znacznie szybszy niż iloczyn kartezjański, ponieważ porównuje
# tylko te pary, które wpadły do tego samego "kubełka" haszującego, zwracając
# przybliżoną odległość Jaccarda.

similarity_threshold = 0.9
# approxSimilarityJoin zwraca pary, których odległość Jaccarda jest MNIEJSZA niż próg.
# Próg 0.9 oznacza, że interesują nas pary o podobieństwie Jaccarda >= 0.1 (1.0 - 0.9).
approx_similarities = model.approxSimilarityJoin(
    danio_vectors,
    mysz_vectors,
    similarity_threshold,
    distCol="JaccardDistance"
).select(
    # Zmieniamy nazwy kolumn dla przejrzystości
    col("datasetA.referenceName").alias("ryba_ref"),
    col("datasetB.referenceName").alias("mysz_ref"),
    col("JaccardDistance")
)

approx_similarities = approx_similarities.coalesce(1)

print("********** 10 Najlepszych Przybliżonych Podobieństw Białek **********")
# Sortujemy po odległości malejąco (im mniejsza odległość, tym większe podobieństwo, ale MinHash podaje odległość, więc malejąco jest ok dla odległości 0 do 1)
# Uwaga: JaccardDistance = 1 - JaccardSimilarity. Niższa odległość to większe podobieństwo.
approx_similarities.orderBy(F.asc("JaccardDistance")).show(10, truncate=False) # Sortowanie rosnąco po odległości (najmniejsza odległość -> największe podobieństwo)
print("******************************************************************")

timing_log["krok_7_przyblizone_laczenie_podobieństwa"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 8: Zapisanie wyników
# ========================================================
step_start = time.time()
# Cel: Zapisujemy przybliżone dopasowania podobieństwa do pliku CSV,
# co pozwala na dalszą analizę poza środowiskiem Spark.

output_csv = f"output/protein_minhash_similarity_{DESIRED_SAMPLE_SIZE}.csv"
approx_similarities.write.mode("overwrite").option("header", "true").csv(output_csv)

timing_log["krok_8_zapisanie_wynikow"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 9: Zapisanie logu czasowego jako JSON
# ========================================================
# Cel: Zapisujemy czasy wykonania, aby monitorować i porównywać wydajność
# metody MinHash LSH (przybliżonej) z metodą dokładną.

timing_log["calkowity_czas_wykonania_sekundy"] = round(time.time() - start_time_total, 2)

# Zapis do pliku JSON z polskimi kluczami
with open(f"output/execution_times_minhash_{DESIRED_SAMPLE_SIZE}.json", "w", encoding="utf-8") as f:
    json.dump(timing_log, f, indent=4, ensure_ascii=False)

print("Podsumowanie czasów wykonania:")
for step, duration in timing_log.items():
    print(f"{step}: {duration} sekundy")

# Zatrzymanie sesji Spark
spark.stop()
