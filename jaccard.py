from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.udfs import generate_tripeptides, compute_jaccard # Zakładam, że te UDF-y są dostępne
import os
import time
import json

# --------------------------------------------------------
# Inicjalizacja sesji Spark
# --------------------------------------------------------
# Inicjalizujemy sesję Spark, która jest niezbędna do uruchomienia operacji na dużych
# zbiorach danych w klastrze lub lokalnie.
spark = SparkSession.builder \
    .appName("Protein Jaccard Exact Calculation") \
    .getOrCreate()

# Utworzenie katalogu wyjściowego, jeśli nie istnieje
os.makedirs("output", exist_ok=True)

# Słownik do przechowywania czasów wykonania poszczególnych kroków.
timing_log = {}
start_time_total = time.time()

# ========================================================
# Krok 1: Wczytanie zbiorów danych i pobranie próbek
# ========================================================
step_start = time.time()
# Cel: Wybieramy małe próbki z dużych zbiorów danych, aby znacząco przyspieszyć
# proces obliczeniowy i umożliwić jego wykonanie w rozsądnym czasie, zamiast
# przetwarzać miliony rekordów. To uproszczenie jest kluczowe dla testów i analizy pilotażowej.

# Wczytanie zbiorów danych białek Danio (ryba) i Myszy
danio = spark.read.parquet("./data/danio_protein.adam")
mysz = spark.read.parquet("./data/mysz_protein.adam")

SEED = 30
DESIRED_SAMPLE_SIZE = 10000
SAMPLING_MARGIN = 0.00005

danio_count = danio.count()
mysz_count = mysz.count()
# Obliczamy frakcję potrzebną do uzyskania żądanej wielkości próbki, dodając mały margines,
# aby zwiększyć szanse na osiągnięcie pożądanej liczby rekordów.
danio_fraction = (DESIRED_SAMPLE_SIZE / danio_count) + SAMPLING_MARGIN
mysz_fraction = (DESIRED_SAMPLE_SIZE / mysz_count) + SAMPLING_MARGIN

# Pobieramy próbki bez zastępowania, używając stałego SEED dla powtarzalności.
danio_sample = danio.sample(withReplacement=False, fraction=danio_fraction, seed=SEED)
mysz_sample = mysz.sample(withReplacement=False, fraction=mysz_fraction, seed=SEED)

timing_log["krok_1_wczytanie_i_probkowanie"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 2: Ekstrakcja sekwencji białkowych
# ========================================================
step_start = time.time()
# Cel: Izolujemy tylko potrzebne kolumny (`referenceName` i `sequence`),
# aby zmniejszyć ilość danych przetwarzanych w kolejnych krokach,
# co optymalizuje operacje PySpark.

danio_sequences = danio_sample.select("referenceName", "sequence")
mysz_sequences = mysz_sample.select("referenceName", "sequence")

timing_log["krok_2_ekstrakcja_sekwencji"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 3: Generowanie 3-merów (trójpeptydów)
# ========================================================
step_start = time.time()
# Cel: Konwertujemy długie sekwencje białkowe na zbiory krótszych, 3-literowych
# fragmentów (trójpeptydów). Jest to kluczowy krok, ponieważ podobieństwo Jaccarda
# jest obliczane na podstawie zbiorów tych fragmentów, a nie na całych,
# długich sekwencjach aminokwasów.

# Rejestracja UDF do generowania trójpeptydów
tripeptides_udf = udf(generate_tripeptides, ArrayType(StringType()))

# Dodanie kolumny z listą trójpeptydów
danio_kmers = danio_sequences.withColumn("tripeptides", tripeptides_udf("sequence"))
mysz_kmers = mysz_sequences.withColumn("tripeptides", tripeptides_udf("sequence"))

# Dodanie kolumny z długością sekwencji - będzie używana do filtrowania par
danio_kmers = danio_kmers.withColumn("sequence_length", F.length("sequence"))
mysz_kmers = mysz_kmers.withColumn("sequence_length", F.length("sequence"))

timing_log["krok_3_generowanie_trojpeptydow"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 4: Generowanie wszystkich par białek mysz–danio
# ========================================================
step_start = time.time()
# Cel: Tworzymy każdą możliwą parę białek (mysz–danio) do porównania (iloczyn kartezjański - crossJoin).
# Następnie filtrujemy te pary, które mają znacznie różniące się długości sekwencji
# (różnica relatywna > 10%), ponieważ są one mało prawdopodobne, aby były homologami.
# To wstępne filtrowanie znacząco redukuje obciążenie obliczeniowe w kolejnym kroku.

mouse_fish_pairs = danio_kmers.alias("mouse").crossJoin(mysz_kmers.alias("fish")) \
    .filter(
        # Filtr sekwencyjny: różnica długości sekwencji musi być mniejsza niż 10%
        F.abs(F.col("mouse.sequence_length") - F.col("fish.sequence_length")) / F.col("mouse.sequence_length") < 0.1
    )

timing_log["krok_4_polaczenie_krzyzowe_i_filtrowanie"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 5: Obliczanie dokładnego podobieństwa Jaccarda
# ========================================================
step_start = time.time()
# Cel: Obliczamy dokładne podobieństwo Jaccarda dla zbiorów trójpeptydów
# każdej pary białek. Podobieństwo Jaccarda jest miarą zdefiniowaną jako
# rozmiar przecięcia podzielony przez rozmiar sumy zbiorów, co pozwala na
# kwantyfikację ich podobieństwa.

# Rejestracja UDF do obliczania Jaccarda
jaccard_udf = udf(compute_jaccard, "double")

# Obliczenie podobieństwa dla każdej pary
similarities = mouse_fish_pairs.withColumn(
    "jaccard_similarity",
    jaccard_udf(F.col("mouse.tripeptides"), F.col("fish.tripeptides"))
)

timing_log["krok_5_obliczenie_jaccarda"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 6: Identyfikacja najlepszego dopasowania dla każdego białka myszy
# ========================================================
step_start = time.time()
# Cel: Grupowanie wyników po białku myszy i wybieranie tylko tego partnera (danio),
# który ma NAJWYŻSZĄ wartość podobieństwa Jaccarda. Użycie funkcji okna
# (`Window`) z rangowaniem (`row_number`) pozwala nam efektywnie zidentyfikować
# najlepsze dopasowanie w obrębie każdej grupy bez konieczności grupowania.

# Definicja specyfikacji okna: partycjonuj po nazwie referencyjnej myszy i sortuj malejąco po podobieństwie
window_spec = Window.partitionBy("mouse.referenceName").orderBy(F.desc("jaccard_similarity"))

# Rangowanie dopasowań i wybór tylko pierwszego (najlepszego)
top_matches = similarities.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .select(
        F.col("mouse.referenceName").alias("mouse_ref"),
        F.col("fish.referenceName").alias("fish_ref"),
        "jaccard_similarity"
    )

print("********** 10 Najlepszych Dokładnych Dopasowań Białek Jaccarda **********")
# Wyświetlenie 10 najlepszych unikalnych dopasowań, posortowanych malejąco
top_matches.orderBy(F.desc("jaccard_similarity")).show(10, truncate=False)
print("**********************************************************")

timing_log["krok_6_rangowanie_i_wybor"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 7: Zapisanie wyników
# ========================================================
step_start = time.time()
# Cel: Zapisujemy ostateczny zbiór danych (najlepsze dopasowania) do pliku CSV.
# Użycie `coalesce(1)` gwarantuje, że wynik zostanie zapisany do jednego pliku,
# co jest wygodne do dalszej analizy.

output_csv = f"output/protein_jaccard_exact_matches_{DESIRED_SAMPLE_SIZE}.csv"
top_matches.coalesce(1).orderBy(F.desc("jaccard_similarity")).write.csv(
    output_csv,
    header=True,
    mode="overwrite"
)

timing_log["krok_7_zapisanie_wynikow"] = round(time.time() - step_start, 2)

# ========================================================
# Krok 8: Zapisanie logu czasowego jako JSON
# ========================================================
# Cel: Rejestrowanie i zapisywanie szczegółowego czasu wykonania każdego kroku
# oraz całkowitego czasu. Jest to kluczowe dla monitorowania wydajności i
# identyfikacji wąskich gardeł w potoku przetwarzania.

timing_log["calkowity_czas_wykonania_sekundy"] = round(time.time() - start_time_total, 2)

# Zapis do pliku JSON z polskimi kluczami
with open(f"output/execution_times_jaccard_{DESIRED_SAMPLE_SIZE}.json", "w", encoding="utf-8") as f:
    json.dump(timing_log, f, indent=4, ensure_ascii=False)

print("Podsumowanie czasów wykonania:")
for step, duration in timing_log.items():
    print(f"{step}: {duration} sekundy")

# Zatrzymanie sesji Spark
spark.stop()
