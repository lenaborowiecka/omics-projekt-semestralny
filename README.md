# Uruchamianie Skryptów Spark w Kontenerze ADAM

## Przygotowanie Danych

Uruchom poniższy skrypt, aby pobrać i przygotować niezbędne pliki danych:

```bash
chmod +x data_preparation.sh
./data_preparation.sh
```

Wszystkie pliki wyjściowe zostaną zapisane w katalogu `data/`.

---

## 1️⃣ Uruchomienie Interaktywnej Sesji Slurm

Jeśli używasz klastra z systemem Slurm, możesz zarezerwować zasoby i uzyskać interaktywną powłokę:

```bash
srun -N1 -n1 -c4 --time=01:00:00 -p topola -A g100-2238 --pty bash
```

* `-N1` → 1 węzeł obliczeniowy
* `-n1` → 1 task
* `-c4` → 4 rdzenie CPU (dopasuj do konfiguracji Spark)
* `--time=01:00:00` → Limit czasu 1 godzina
* `--pty bash` → Uruchomienie interaktywnej powłoki

---

## 2️⃣ Wejście do Kontenera ADAM

Użyj Apptainer (dawniej Singularity), aby wejść do środowiska kontenera zawierającego ADAM i Spark:

```bash
apptainer shell --overlay overlay docker://quay.io/biocontainers/adam:1.0.1--hdfd78af_0
```

---

## 3️⃣ Uruchamianie Skryptów Spark

Możesz uruchomić skrypty lokalnie (interaktywnie) w kontenerze:

```bash
time spark-submit --master local[4] jaccard.py
time spark-submit --master local[4] minhash.py
```

* `--master local[4]` → używa 4 rdzeni CPU
* `time` → mierzy czas wykonania
* Wyniki są zapisywane w katalogu `output/`.

---

## 4️⃣ Automatyzacja Benchmarku: Uruchamianie dla różnych rdzeni

Do mierzenia **skalowalności** (czas i pamięć w zależności od liczby rdzeni) przygotowano skrypt:

```bash
./benchmark_skalowania.sh
```

Skrypt automatycznie uruchamia Slurm i Apptainer dla wielu konfiguracji rdzeni, zapisując wyniki w katalogu `output/`.

---

## 5️⃣ Slurm: Skrypty wsadowe

W repozytorium znajdują się dwa gotowe skrypty `.sbatch` do uruchomienia zadań Slurm:

* **`go_jaccard.sbatch`** – uruchamia `jaccard.py`
* **`go_minhash.sbatch`** – uruchamia `minhash.py`

Przykładowe uruchomienie:

```bash
sbatch go_jaccard.sbatch
sbatch go_minhash.sbatch
```

Każdy skrypt:

* Rezerwuje zadeklarowaną liczbę rdzeni (`-c 1,4,8,12,16`)
* Uruchamia kontener Apptainer ze Spark
* Zapisuje wyniki w katalogu `output/` (`result_jaccard_*.out`, `result_minhash_*.out`)
* Tworzy logi Slurm w `output/slurm_*.out` i `output/slurm_*.err`

---

## 6️⃣ Wyniki i Analiza

Po zakończeniu wszystkich zadań:

* Pliki wynikowe w `output/` zawierają:

  * Czasy wykonania poszczególnych kroków
  * Maksymalne użycie pamięci RAM
  * Top 10 dopasowań białek (`jaccard_similarity`)
