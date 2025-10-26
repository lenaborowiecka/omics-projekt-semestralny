# Uruchamianie Skryptu Spark w Kontenerze ADAM

## Przygotowanie Danych

Uruchom poniższy skrypt, aby pobrać i przygotować niezbędne pliki danych:

```bash
chmod +x data_preparation.sh
./data_preparation.sh
```
---

Wykonaj poniższe kroki, aby interaktywnie uruchomić skrypty Spark (`jaccard.py`, `minhash.py`) wewnątrz kontenera.


## 1️⃣ Uruchomienie Interaktywnej Sesji Slurm

W systemie zarządzania zasobami Slurm (jeśli używasz klastra) użyj następującego polecenia, aby zarezerwować zasoby i uzyskać interaktywną powłokę:

```bash
srun -N1 -n1 -c4 --time=01:00:00 -p topola -A g100-2238 --pty bash
```

* `-N1` → 1 węzeł obliczeniowy
* `-n1` → 1 task
* `-c4` → 4 rdzenie CPU (dopasuj to do konfiguracji Spark)
* `--time=01:00:00` → Limit czasu: 1 godzina
* `--pty bash` → Uruchomienie interaktywnej powłoki

---

## 2️⃣ Wejście do Kontenera ADAM

Użyj Apptainer (dawniej Singularity), aby wejść do środowiska kontenera, który zawiera wszystkie niezbędne narzędzia bioinformatyczne (w tym ADAM i Spark):

```bash
apptainer shell --overlay overlay docker://quay.io/biocontainers/adam:1.0.1--hdfd78af_0
```

---

## 3️⃣ Uruchomienie Skryptu Spark

Użyj narzędzia `spark-submit`, aby wykonać skrypt Python, wskazując, ile rdzeni ma być użytych w trybie lokalnym:

```bash
time spark-submit --master local[4] jaccard.py
time spark-submit --master local[4] minhash.py
```

* `--master local[4]`: Użyj 4 lokalnych rdzeni do przetwarzania (dopasuj do parametru `-c4` ze Slurm)
* `time`: Wbudowane polecenie bash do pomiaru czasu wykonania całego procesu.
* Wyniki (pliki CSV i JSON z czasami) zostaną zapisane w katalogu output w bieżącym folderz
