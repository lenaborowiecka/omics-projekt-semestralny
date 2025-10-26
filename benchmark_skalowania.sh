mkdir output

for script in go_jaccard.sbatch go_minhash.sbatch
do
  for c in 1 4 8 12 16
  do
    sed -i "s/^#SBATCH -c .*/#SBATCH -c $c/" $script
    sbatch $script
    echo "Submitted $script for $c cores"
  done
done

