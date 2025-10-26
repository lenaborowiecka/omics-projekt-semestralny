#!/bin/bash
# setup_proteins.sh
# This script installs unprivileged Apptainer if needed, downloads protein FASTA files,
# decompresses them, renames them, creates overlay, and converts them to ADAM format.

set -e  # Exit on any error

# -------------------------------
# 1. Install unprivileged Apptainer if not already installed
# -------------------------------
if [ -d "install-dir" ]; then
    echo "Apptainer already installed in 'install-dir', skipping installation..."
else
    echo "Installing unprivileged Apptainer..."
    curl -s https://raw.githubusercontent.com/apptainer/apptainer/main/tools/install-unprivileged.sh | bash -s - install-dir
fi

# -------------------------------
# 2. Download protein FASTA files
# -------------------------------
echo "Downloading protein FASTA files..."
wget -q https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/635/GCF_000001635.27_GRCm39/GCF_000001635.27_GRCm39_protein.faa.gz
wget -q https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/049/306/965/GCF_049306965.1_GRCz12tu/GCF_049306965.1_GRCz12tu_protein.faa.gz

# -------------------------------
# 3. Decompress the downloaded files
# -------------------------------
echo "Decompressing protein FASTA files..."
gunzip -f ./GCF_000001635.27_GRCm39_protein.faa.gz
gunzip -f ./GCF_049306965.1_GRCz12tu_protein.faa.gz

# -------------------------------
# 4. Rename files for convenience
# -------------------------------
echo "Renaming protein files..."
mv GCF_000001635.27_GRCm39_protein.faa mysz_protein.fa
mv GCF_049306965.1_GRCz12tu_protein.faa danio_protein.fa

# -------------------------------
# 5. Create overlay directory for Apptainer
# -------------------------------
echo "Creating overlay directory..."
mkdir -p overlay

# -------------------------------
# 6. Enter Apptainer container and Convert FASTA to ADAM format
# -------------------------------
echo "Entering Apptainer container..."
install-dir/bin/apptainer shell --overlay overlay docker://quay.io/biocontainers/adam:1.0.1--hdfd78af_0 << 'EOF'

echo "Transforming protein FASTA to ADAM..."
adam-submit transformAlignments mysz_protein.fa mysz_protein.adam
adam-submit transformAlignments danio_protein.fa danio_protein.adam
echo "Protein conversion complete!"
EOF

# -------------------------------
# 7. Move all files to ./data directory
# -------------------------------
mkdir -p ./data
mv mysz_protein.fa ./data/mysz_protein.fa
mv danio_protein.fa ./data/danio_protein.fa
mv mysz_protein.adam ./data/mysz_protein.adam
mv danio_protein.adam ./data/danio_protein.adam

echo "All steps finished successfully."
